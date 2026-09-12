# Tool, Reload And Stateless Subagent Acceptance

Accepted source: **`f1697f78ce48c8f5f3fde0dc9903c153022bfd9e`**, paired with TUI
**`30ea7ca828f59cab895486beebc98e4e4f4ecf2e`**. Agent **0.3.3**, TUI **0.2.8**,
and Runtime **0.4.1** revision `6cd2bdbc634437dea925495c61c7eb0be10ba171` remain
unchanged. These are separate local commits, not a new tag, push or hosted-CI run.

| Commit | Independently reviewed scope |
|---|---|
| `fbae60f` | Safe bounded live Tool failure bodies |
| `f5abe83` | Bounded single-existing-file Codex Update File support alongside unified patches |
| `f08984b` | Source-aware `agent.reload`, atomic candidate installation and cumulative credential-name scrubbing |
| `f1697f7` | Native stateless subagent single/parallel/chain |

The parent built fresh Git archives on **`root@192.168.20.199`**. Final stable and
Rust 1.85 all-target suites each report **369 passed / 0 failed / 2 ignored**
(343 library plus 26 integration tests). Stable fmt, strict Clippy, warning-denied
rustdoc and builds passed. Paired TUI suites each report 498 passed / 19 ignored;
all 18 real-Agent loopback E2E tests passed separately on stable and MSRV.
Current evidence is indexed by [followups/provenance.json](followups/provenance.json)
and checksummed in `followups/FILES.sha256`. Existing MSRV strict-Clippy diagnostics
are not claimed as fixed. No new Windows execution is claimed.

## Native And Installation

Remote Rust 1.98.0 / Clang-LLD 19 produced macOS Debug and Release artifacts.
Both passed Mach-O/signature checks and matching Debug/dSYM UUID checks. Native
followup runs each made 19 loopback HTTP requests through actual TUI/Agent
binaries. They verify failure-body folding, a real patch and whole workspace
content/entry-set comparison, all three subagent modes, strict actual HTTP tool
schema, child tool isolation, inherited prompt/reasoning, real outputs/usage,
chain substitution, no child Store Sessions, and read-only idle reload followed
by changed model requests. See `followups/native/{debug,release}/`.

Separate paired native Session/stream regressions and real-TTY normal/panic
restoration passed. The complete paired evidence is in the sibling TUI repository
at `docs/verification/followups/`; this Agent directory is an explicitly scoped
subset. Native means real iTerm2 screen text/input and loopback providers, not
external-provider/TLS, pixels, macOS 11 hardware, or native busy-reload acceptance.

Accepted Debug/Release binaries and Debug symbols are installed in the existing
Agent `target/debug/` and `target/release/` locations. Old executable inodes,
hashes and symbols are preserved under `target/preserved-before-followups-AdCugX/`.
[Installation evidence](followups/installation.log) records byte comparison and
UUID checks. No user process was restarted and installation did not change user
configuration or Store data. Already-running processes may still use old images.

## Contracts And Limits

`Agent::open_file` retains the absolute lexical startup path for reload;
`Agent::open` remains source-less. Reload prebuilds the candidate before swapping,
keeps active loop snapshots, and does not rewrite Session records/history. New
Sessions use new profiles; existing Sessions retain stored prompt/tool/approval
snapshots. The TUI's `/reload` is not a binary reload. To enable delegation, add
`subagent` to the intended profile's existing tools, reload, and create a new
Session; existing approval policy still applies.

The native tool runs stateless child AgentLoops without child Store Sessions.
It supports single, up to eight task/chain stages, at most four parallel children,
configured-model/reasoning overrides and bounded output. Cwd remains inside the
parent workspace; children physically lack recursive `subagent`. Unknown usage
and counters are not fabricated as zero or known totals. Normal completion joins
children; external Tool/turn drop cancels them and retains handles for later
Session/Agent drain. Reliable Runtime watch state detects unserviceable child
approval/input waits independently of best-effort events.

Persistent alias/target, Session adoption/fork/exclusive, steer/followUp controls,
and a manager panel are **not implemented**. Compaction remains planning-only.
`apply_patch` supports a single existing file, not full Add/Delete/Move or every
Git preamble. Original user failure payloads were not read; the native and process
fixtures establish synthetic format compatibility, not the unique original cause.

## Evidence Integrity

Earlier apply-patch development violated the remote-only restriction by executing
local Cargo/Rust and overwriting Agent binaries. It was disclosed, rejected local
results were excluded, and accepted remote binaries were restored before this
final delivery. See `followups/audit/local-execution-violations.tsv`; rejected
executables remain in `target/preserved-unauthorized-local-AdCugX/`. Local
`apply-patch-final-*` logs are not remote validation. All accepted final source
checks and delivery builds were rerun by the parent on the authorized builder.
The final reviewer reported two temporary checksum-output files created and
removed during its audit, without repository/Git/source/user-artifact changes;
that audit is not described as literally write-free.

The TUI archive retains behavioral REDs, compilation/setup failures, and an
initial native harness failure separately from final PASS evidence. The latter
waited for a patch completion marker below an anchored viewport even though the
file/provider result were already correct. The final functional harness follows
new output; independent scrolling assertions were not relaxed. Final native
scripts include their frozen loopback helper. No user configuration, user Store
or credentials were copied into this evidence subset.
