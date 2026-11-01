# Test-only Agent observations

The default build has no observer, fault-plan reader, trace writer, or telemetry
counters. `--features e2e-observe` compiles narrow instrumentation for isolated
real-stdio tests; it is not a product configuration or an RPC extension.

## Activation

Set `MINICORE_AGENT_E2E_TRACE` to a new private JSONL file. Optionally set
`MINICORE_AGENT_E2E_PLAN` to a startup-only JSON plan (at most 64 KiB / 64 steps)
and `MINICORE_AGENT_E2E_RELEASE_DIR` to an existing private directory for pause
release files. Without the trace variable, instrumentation is inactive; setting
fault variables without a trace is rejected. No existing trace is overwritten.
Only literal loopback provider IPs are accepted while observing. Use dummy keys
and the existing loopback mock. All three variables are scrubbed from tools.

The CLI owns and joins one bounded trace writer after stdio, Session, query,
Tool owner, observer, and writer-buffer shutdown. Limits are 8 KiB per row,
256 queued rows, and 16 MiB / 65,536 rows. Execution never waits for trace file
I/O or queue capacity. Overflow, write failure, unconsumed faults, and explicit
gate timeout invalidate observation and produce a nonzero test-process exit.
There is no observer polling when idle; only an armed test pause probes its
release file, under a finite timeout and the existing cancellation path.

## Measurements

Rows use `schema: "agent-e2e-1"`, contiguous writer `event_seq`, and `mono_ns`
from one Agent-process `Instant` origin. Actual snapshot sample, encoding, and
flushed stdio write rows carry session/load/revision and stage timing. These are
not TUI application timestamps; do not subtract clocks across processes.
Concurrent rows can be enqueued in a different order from their capture times.
`observer_ready` advertises `gauge_order: "per-gauge-revision-v1"` and
`query_order: "query-id-v1"`; historical traces without them have no causal
metadata guarantee.

Each nonzero gauge update contains `name`, `revision`, `previous_current`,
`action` (`add`/`sub`), `amount`, `current`, `peak`. One per-gauge leaf mutex
mutates and captures those fields together; encoding and enqueue happen outside
it. Revisions start at 1 and are contiguous. Checked value/revision overflow or
underflow invalidates observation without changing the last valid state.
Checkpoints hold a coherent `{revision,current,peak}` for each gauge separately;
revision 0 is the actual initial zero state. Reconstruct by per-gauge revision,
validate the action/amount transition and bind every checkpoint version to its
recorded state. The final version must be the maximum observed version with
current zero. The last enqueued gauge row need not describe the final state.

`gauge` rows and final `counters` measure current/peak ownership transitions:
Runtime watch subscriptions owned by the turn driver; charged source bytes/count
(including evicted sources held by pages); catalog source bytes/count, metadata
allocation charge/entries; control
staging reservations/bytes until flush; retained page refs and reference-weighted
bytes; active owner objects, lightweight completion values, late-wait slots,
actual wait receiver handles, transient driver report owners, prepared result
queries, compact stored
identity bytes/count, and transient page-prefix workspace bytes. Compact identity
charges survive eviction until the last page reference; close prevents reinsertion.
The index holds no committed body or interrupted text. These are
charged/owned resources, not RSS or a whole-process heap limit. A peak can
include short-lived transition overlap. Shared source charges count once;
reference-weighted bytes deliberately count each page reference.

Work rows count durable/public normalization calls/items/time, source
preparation calls/items/encoded bytes/time, source hash input bytes/items/time,
borrowed result projection/hash calls/input bytes/items/time, stored-record
decode, cached range encoding, page-prefix scanning and query encoding. Work rows
include `started_ns`/`finished_ns`; `responsiveness_probe` marks actual ping
handling, so overlap with CPU work can be checked within this Agent clock.
Normalization
byte counts are omitted because they are not measured. Hash bytes include the
actual digest framing; combined projection/hash durations include projection,
counting, and encoding, not only SHA instructions. Query rows report the final
cancellation-normalized result at the existing query-owner boundary. Successful
page replies are split into available, pending, and unavailable counters; none
of these counters asserts that a frontend installed a complete body.

Source preparation includes its nested source hash, so durations are not
additive exclusive CPU phases. Each admitted Query captures its exact requested
session/load/loop, cursor and `requested_result_id`. Its unique `query_id` comes
from the checked increment of the actual `result_queries` total. One outcome row
per ID includes the actual returned `result_id`, `next_cursor`, `total`,
`availability`, `unavailable_reason`, safe `error` classification and chunk counts,
including cancellation and `awaiter_dropped`. Error rows have no returned page;
null fields and zero counts do not imply a completed empty body. Outcomes may
finish in any order; at quiescence IDs 1..N occur exactly once and totals agree.
No per-ID producer history is retained beyond the live Query.

Active/completion/late-wait gauges describe Agent owner objects/slots. Completion
and late-wait values retain terminal summaries, not Runtime reports. The driver
report gauge lasts through persistence and tail settlement; the watcher gauge
counts an in-task subscription, not a separate observer task. Caller-retained
Rust Arcs are outside Agent ownership. Gauge fields are sampled independently;
compare gated/quiescent
checkpoints for cross-resource invariants, not a transactional memory snapshot.
No raw prompt, answer, argument, command, provider body, secret, or path is
written into the trace. Trace consumers reject missing ready/end, discontinuous
sequence, drops, or invalid completion.

## Narrow fault plan

```json
{
  "schema": 1,
  "timeout_ms": 10000,
  "limits": {"sources_per_load": 1, "sources": 1},
  "steps": [
    {"id":"rollback-a","session":1,"turn":1,"at":"append","action":"rollback"},
    {"id":"hold-page","session":1,"turn":1,"at":"result_page","action":"pause"}
  ]
}
```

Targets are registration/installed-turn ordinals, not text or foreground state;
reopen gets a new registration ordinal. Real opaque identities are also traced.
The observer bounds its identity table to 64 current loads and 64 recent turns
per load; overflow invalidates telemetry without limiting product Sessions.

- `append/rollback` writes/flushed half a real record, then invokes existing
  truncate/sync rollback. `append/unknown` instead exercises rollback failure
  through a read-only handle. Existing append ownership and recovery remain.
- `runtime_events/drop_terminal` holds the selected Runtime best-effort stream
  until join and discards its bounded buffered tail. Runtime watch, execution,
  direct Tool observations, owners, canonical report, and join are unchanged.
- `before_persist/pause` holds after Runtime/Tool join and before append.
- `result_page/pause` holds an acquired immutable source Arc before assembly.

For a pause, wait for `fault_ready`, perform the concurrent action, and create
`<release_dir>/<id>.release`. IDs are unique 1–32 byte ASCII alphanumeric/`-`/`_`
tokens. The existing cancellation path can drop/release the pause and is traced
explicitly. Session-close queries retain `session_not_loaded`; caller/shutdown
cancellation retains `query_limit`. Existing query deadlines can precede the
gate deadline. `timeout_ms` is 50–30,000, default 10,000. Limits may only lower
existing positive result/catalog limits; no arbitrary runtime mutation API is
provided. Stdout frames are never reordered or replaced for a test.

An integration mirror may forward its TUI `e2e-observe` feature to
`minicore-agent/e2e-observe`; do not change a formal release-tag dependency to a
feature unavailable on that tag. Agent-specific environment variables must
reach the Agent child, while the TUI's separate trace variable must not.

## Actionable input fixture

Set `"input_fixture": true` in the same schema-1 startup plan to advertise the
feature-only `e2e_input` tool to the actual Runtime ToolSet. Do not put it in the
profile tool list. The observer and literal-loopback checks remain required.
Its only argument is required `kind: "text" | "single_choice"`, with no extra
properties. Fixed prompts are `E2E text input` and `E2E single choice`; choices
are `First choice`, `Second choice`.

The tool returns Runtime's real `RequestInput` outcome. Runtime installs the
actual pending identity, validates ordinary `interaction.answer` requests and
continues the same tool call with `InputProvided`. Text produces `{answer:...}`;
choice index 1 produces `{choice_index:1,choice:"Second choice"}`. Nothing
synthesizes a snapshot or replaces Runtime validation. Wrong-kind/index answers
leave the pending interaction operable. Normal cancellation and owner joins apply.
`input_fixture_request`/`input_fixture_answer` rows contain exact identities,
answer kind/count/index/bytes and acceptance, never answer text. Without this
startup flag, even an observed feature build advertises only product tools.
