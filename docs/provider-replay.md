# Durable Responses Replay

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

Text consistency uses Runtime 0.7.0 normalization while raw replay remains exact:
CRLF/CR become LF and controls other than LF/TAB become one space. Runtime
normalizes split CRLF across deltas and adapter chunks to one LF, so canonical
text remains consistent with the combined raw message and replay is retained.

A different endpoint/model or unknown format falls back to canonical parts.
An opaque-only item has no canonical fallback, so incompatible replay fails
locally rather than silently dropping context. Known malformed/stale payloads
are rejected, including when the selected identity differs. Old history with
no attachment or a null attachment remains readable.

Responses requests include `reasoning.encrypted_content` only when reasoning is
not disabled and the model configuration supports an explicit reasoning level.
Final text, tool, and reasoning output can be captured regardless of that request field. A terminal reasoning item may add a previously
missing/null encrypted-content string only if every other field is identical;
existing ciphertext is never replaced. An encrypted-only response can survive
as an empty canonical assistant with real validated replay. No visible text is
invented, ordinary empty constructors still reject, and completion/refusal/
output-limit policy is unchanged. This compatibility repair is not a proven
provider cache-hit fix.

A missing message/reasoning item ID disables raw replay for that item, not the
response. Anonymous reasoning (including ciphertext) is omitted; anonymous messages
use only canonical text without an input message ID. Other identified items
remain replayable. Malformed IDs that are present are still rejected. An anonymous
opaque-only response fails because it has neither usable replay nor canonical content.

## Bounds, persistence, and privacy

The entire serialized Runtime replay envelope is limited to 256 KiB, depth 32,
and 16,384 JSON nodes. Agent permits at most 256 output items per response.
Replay bytes count toward the shared Runtime/Agent 2 MiB history limit;
the existing 16 MiB JSONL record cap remains. No ciphertext is truncated. When
capture exceeds a Runtime replay bound (including envelope framing), the adapter
omits the entire replay attachment and keeps non-empty canonical text, reasoning,
and tool calls from the already-billed response. A replay-only response still fails
if its attachment cannot fit. Provider shape, canonical text/tool consistency, and
the 256-item limit remain enforced; overflow is not permission to accept malformed
provider output. Compact SHA-256 reconciliation evidence and item IDs survive raw
replay disposal, so terminal output cannot rewrite completed items or introduce
duplicate IDs after overflow.

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
skipping it with a synthetic model. The shared token heuristic takes the maximum
of rounded-up serialized bytes / 4 and non-ASCII character count; utility request
estimates retain their framing margin. It is not an exact provider-token count.
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
are replaced by attachment-based tests in `src/models/openai/tests/replay_tests.rs`,
`tests/openai_rpc_process.rs`, and the emergency Agent tests. There is no such
cache or eviction lifecycle in this version.

No real-provider, paid-model, cache-hit-rate, native cross-platform, or remote CI
claim follows from these local tests. Old live-provider results remain historical.
