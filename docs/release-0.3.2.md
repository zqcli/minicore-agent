# minicore-agent 0.3.2 patch notes

Scope: bounded to (a) the SSE reasoning summary boundary fix and (b) read-only
Steer receipts over the existing `PromptProvider` seam. The Runtime is
untouched. No cross-platform or pixel claims.

## Reasoning summary part boundaries (src/models/openai.rs)

`response.reasoning_summary_text.delta` is parsed with its part identity
(`item_id`, `output_index`, `summary_index`) instead of discarding these fields. When the identity changes — or a reasoning item's `output_item.added`/`done`
arrives — exactly one newline is emitted between the two nonempty parts. Rules:

- fragments inside one part stay concatenated
- empty deltas and duplicate lifecycle events add no lines
- raw provider newlines at a boundary are never doubled
- legacy deltas with no boundary identity stay concatenated (previous behavior)
- ordinary assistant/refusal text, raw output-item replay/continuation, tool
  flow, and error redaction are unchanged

Covered by parser and streaming tests in `src/models/openai/tests.rs`, including
summary-part lifecycle boundaries, missing metadata within a known part, and
an empty delta announcing a new part. Missing fields alone do not create a line
break. Raw replay payloads are unchanged.

## Steer receipts (read-only)

- `SteerAccepted`/`SteerResult` gain an optional 1-based `steer_index` assigned
  by the per-session `Presentation` accepted counter in FIFO acceptance order
  (serialized by the existing session lock; reset on loop start).
- `SteerReceiptPrompt` wraps the existing `ProjectPromptProvider` and counts
  Steering User items for the request's loop in the PREPARED prompt history —
  retained only after a successful prepare, so rebuilding/repairing alone never
  releases the queue.
- `PresentationModel::start` commits the matching prepared count at the real
  `Model::start` and emits `steer_progress {turn, request_index, applied_count}`
  only for a newly observed count; later requests with the same count keep the
  first-application boundary.
- The latest receipt is exposed in the existing `session.presentation`
  optional `steer_progress` field (no new RPC method) for lost-event
  reconciliation.
- Metadata only: no prompt messages are rewritten, no User text is logged, and
  execution semantics are unchanged (existing direct RPC batching still applies
  when steers are issued without pacing).

## Tests

Stable and Rust 1.85 all-target suites each passed **279 tests / 2 ignored**.
Formatting, all-target Clippy and docs (warnings denied) passed. Debug and
Release binaries were rebuilt. The TUI 0.2.4 real-Agent suite passed **16/16**,
including the unchanged direct batching contract and the new client-paced FIFO.

Real iTerm2 3.6.11 also exercised this binary with a synthetic loopback provider:
summary parts without raw newlines rendered separately, the Alpha request
excluded Beta, and the subsequent Beta request included Alpha's answer. No
external provider was called; pixel screenshots were not verified.

Restart the Agent child to activate the parser fix. Already-flattened historical
reasoning strings are not rewritten.