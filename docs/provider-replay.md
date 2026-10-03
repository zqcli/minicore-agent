# Durable Responses Replay (0.6.1)

Provider replay is an optional attachment to an assistant history item, not a
second session history manager or a request cache. The existing Runtime history,
Agent in-memory history, and StoredLoopRecord JSONL are the only pipeline.

## Authority and compatibility

Runtime treats the attachment as bounded opaque data with redacted Debug. Agent
owns `openai-responses-v1`: normalized Responses endpoint (including its path),
actual provider model, and ordered raw output items. Credentials are never in
the identity. Only assistant messages, reasoning, and function calls are allowed;
input messages, developer/user roles, and tool results cannot be injected.

Known payloads validate shape, unique IDs/order, canonical text and canonical
call IDs/names/JSON arguments before use. Matching replay atomically replaces the
whole canonical assistant request projection. The separately stored tool result
is sent once. Canonical parts alone control tool execution and display.

A different endpoint/model or unknown format falls back to canonical parts.
An opaque-only item has no canonical fallback, so incompatible replay fails
locally rather than silently dropping context. Known malformed/stale payloads
are rejected, including when the selected identity differs. Old history with
no attachment or a null attachment remains readable.

Responses requests include `reasoning.encrypted_content`. Final text, tool, and
reasoning output can be captured. A terminal reasoning item may add a previously
missing/null encrypted-content string only if every other field is identical;
existing ciphertext is never replaced. An encrypted-only response can survive
as an empty canonical assistant with real validated replay. No visible text is
invented, ordinary empty constructors still reject, and completion/refusal/
output-limit policy is unchanged. This compatibility repair is not a proven
provider cache-hit fix.

## Bounds, persistence, and privacy

The entire serialized Runtime replay envelope is limited to 256 KiB, depth 32,
and 16,384 JSON nodes. Agent permits at most 256 output items per response.
Replay bytes count toward the Runtime and mirrored Agent 2 MiB history limit;
the existing 16 MiB JSONL record cap remains. No ciphertext is truncated.

Durable normalization retains validated replay. Public RPC, session/turn reads,
export-like encoded items, Debug, and summary source prose remove it. Compaction
folds existing history groups: covered items disappear from the next request,
while unsummarized tail items retain their attachments. Summary prose does not
contain ciphertext. Actual history-to-model projection remains one item per
message so raw fold indices stay aligned. Append failure does not merge history.
Legacy records without replay are not needlessly rewritten.

Actual model admission and recovery estimates serialize through exactly the
same model/endpoint-aware request projection as sending. The generic utility
size estimate is deliberately conservative, uses maximum framing allowances,
and includes valid replay regardless of identity instead of accidentally
skipping it with a synthetic model. It is not an exact provider-token count.
The utility's source prose itself remains redacted.

## Verification boundaries

The deterministic HTTP/RPC suite checks same-loop tool replay, the next user
turn, and a fresh OS process reopening JSONL, plus public/log redaction. The
nine-request emergency scenario covers retry identity and covered-prefix /
retained-tail behavior. Replay unit tests cover identity fallback, stale calls,
roles, ordering, terminal enrichment/refusal, unknown formats, bounds, EOF and
cancellation. Read pagination and append-failure tests exercise opaque-only
history. Runtime independently covers old constructors/serde and exact byte
admission.

The removed tests targeted implicit loop-local continuation eviction, cancelled
loop purging, request-index sequencing, and a 4 MiB cache. Their relevant output
order/argument, terminal mismatch, failure, isolation, and byte-limit assertions
are replaced by attachment-based tests in `src/models/openai/replay_tests.rs`,
`tests/openai_rpc_process.rs`, and the emergency Agent tests. There is no such
cache or eviction lifecycle in this version.

No real-provider, paid-model, cache-hit-rate, native cross-platform, or remote CI
claim follows from these local tests. Old live-provider results remain historical.
