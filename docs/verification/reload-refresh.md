# Paired Public Reload Correction

Current installed source pair: unchanged Agent
**`f1697f78ce48c8f5f3fde0dc9903c153022bfd9e`** / TUI
**`a604e55baf74422722545b86bb9c6d30c31473e6`**. Versions remain Agent **0.3.3**,
TUI **0.2.8**, Runtime **0.4.1** at `6cd2bdbc634437dea925495c61c7eb0be10ba171`.

The final public-command audit found that the previous TUI `/reload` did not issue
the old `/refresh` command's exact `turn.wait` reread. Its internal refresh tests
had not proved the public bridge. TUI `a604e55` adds the omitted one-shot read,
without new wire methods or Agent changes. Configuration reload does not wait for
that read to complete. Exact TurnRef deduplication and current-turn ownership also
prevent a delayed old result from replacing a newer completion, or an old wait
failure from clearing a newer handoff and duplicating queued input.

The parent executed fresh exact-source verification on `root@192.168.20.199`:
TUI stable/MSRV **508 passed / 19 ignored**, all **18 real-Agent E2E tests** separately
on both toolchains, and stable fmt/strict Clippy/warning-denied rustdoc/build gates.
Agent's Linux binary was rebuilt from its unchanged fixed source archive for E2E.
Its prior **369 passed / 2 ignored** all-target acceptance remains valid and is not
relabeled as a new run; see [the earlier Agent acceptance](followups.md).

All six native Debug/Release feature, Session and streaming workflows were rerun,
plus real-TTY normal/panic restoration with exits 0/101 and unchanged `stty`.
Feature runs each made 19 loopback HTTP requests. Corrective TUI binaries and Debug
symbols were installed; Agent macOS binaries were reused and remain byte-identical:

| Agent artifact | Unchanged SHA-256 |
|---|---|
| Debug | `443b88b385d778a41303540f940718cf85968a375ca0f334437f9ff42c4703e6` |
| Release | `57a1c3722cf05c251f499b8a1e4d50032e9c8b3ce9e4efcd27028ff4dfc73914` |

Complete paired corrective evidence is in the TUI repository at
`docs/verification/reload-refresh/`, including `README.md`, `provenance.json`,
`FILES.sha256`, exact-source/draft logs, six native result directories, TTY evidence,
and installation records. Large binaries and source archives are in
`/tmp/minicore-followups.AdCugX/reload-refresh/`. TUI's immediately previous files
are preserved under `target/preserved-before-reload-refresh-AdCugX/`; prior backups
in both repositories remain intact. Existing Agent `followups/` manifests and raw
checksums retain their original TUI `30ea7ca` pairing and are not rewritten.

No configuration/Store data was edited, user process restarted, version bumped,
tag created, push performed or new hosted-CI/Windows result claimed. An existing
process may still run an older image; `/reload` does not reload executable code.
Forced late-result races are command/RPC-flow tests, not a new native busy-reload
or real-upstream/TLS claim. Earlier workflow violations remain disclosed in the
original reports. Stateless delegation is still Stage 1; persistent orchestration
and compaction are not implemented.
