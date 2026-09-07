# Agent 0.3.3 Verification

Paired with TUI 0.2.8. Compilation was remote-only in
`/root/minicore-release-028-033.Fm9vbA`; no user configuration, store or real
provider was used. No additional Agent Rust behavior was changed during the
version update; the known limitations in [release notes](../../release-0.3.3.md)
remain open.

- Stable and Rust 1.85: **280 passed / 2 ignored each**.
- Fmt, all-target Clippy with warnings denied, and rustdoc with warnings denied passed.
- Debug/Release macOS x86_64 artifacts were built with Rust 1.98, Clang/LLD 19,
  macOS SDK 26.2 and deployment target 11.0. SDK headers support ring's C build.
- Native Debug and Release pairs each reported `agent.ping` **0.3.3** and passed
  the TUI's real-iTerm2 interaction driver, including actual loopback requests,
  tool execution, reasoning boundaries and paced Steer delivery.
- TUI's separate Linux real-Agent loopback E2E suite passed **16/16**. Its logs,
  native driver, screen evidence and TTY restoration checks are in the TUI
  repository's `docs/verification/0.2.8/`.
- Installed executables match the native-tested hashes. Mach-O/signature checks
  passed; Debug/dSYM UUIDs agree. Previous files remain in
  `target/preserved-before-release-ew5WEt/`.

`remote/` contains raw Agent gate/build logs. `native/debug.json` and
`native/release.json` contain the final paired native results and hashes.
`installed-builds.txt` records installed/preserved artifacts; `source-local.txt`
and `source-remote.txt` are identical.

Native pixels, execution on macOS 11 itself, real-upstream TLS behavior, and new
GitHub Actions platform results are not certified by this local acceptance.
The first TUI driver attempt had a test-variable collision, fixed only in that
harness; the final paired runs passed. No release tag was created.
