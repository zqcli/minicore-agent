# Compaction Implementation Verification

Implementation follows `docs/compaction-plan.md` in independently verified slices.
The `cus-resp/gpt-5.6-luna:max` helper develops source only; the parent reviews,
orchestrates, runs remote verification, and stages/commits. Package versions and
Runtime 0.4.1 revision `6cd2bdbc634437dea925495c61c7eb0be10ba171` are unchanged.
No push or tag is performed. Previously installed binaries are not replaced by
these intermediate source checkpoints.

## Snapshot Loading Foundation

Implemented: bounded `summary.json` loading on Session open, strict derived-data
validation, and non-system context projection in actual next model requests.
This is not yet summary generation, snapshot persistence, manual RPC/UI,
automatic compaction or context-overflow recovery.

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
`ModelRequest`s. No user conversation or private configuration was used.

| Gate | Result |
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

All Rust commands ran on `root@192.168.20.199` under
`/root/minicore-compaction.j5Jaqn/source/`, with parent-owned logs in the same
root's `logs/`. Local staging is `/tmp/minicore-compaction.IpojFx/`. Source-only
helpers ran no Rust/Cargo/fmt, SSH, process management or Git writes.
`sha2 = "=0.10.9"` and its lock additions were resolved/fetched remotely; existing
locked package versions were not upgraded. Subsequent gates used `--locked --offline`.

Before the first Rust command, only six regenerable incremental directories in
three explicit owned caches were removed: 5,164,897,898 logical bytes. All 1,542
retained artifact hashes were checked unchanged. Free space became 31,633,502,208
bytes. The Runtime Cargo process at `/root/minicore-runtime-v04-build/phase2`
and its separate directory were preserved. See `compaction/cleanup.json` and
`compaction/cleanup-preserved-hashes.json`.

Raw evidence/scripts are under `compaction/`; `compaction/FILES.sha256` covers their
bytes. This checkpoint does not claim new hosted CI, Windows, native iTerm2,
macOS artifacts or real-upstream acceptance. Historical MSRV strict-Clippy
limitations are not suppressed or relabeled as fixed.
