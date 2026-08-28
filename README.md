# MiniCore Agent

This repository is the Phase 1 RPC-first skeleton: one Rust package with a
library API and the `minicore-agent` stdio binary. It is verified against the
`minicore-runtime` `dev` HEAD
`7e85eaab18e273e43e03c50040b460f1b13f0ac9` through
`tests/runtime_api_compile.rs`. The runtime dependency is pinned to that exact
Git revision; the local sibling checkout is used only for API review and is not
modified here.

## Run

```bash
minicore-agent --config ./example.agent.toml --stdio
```

The current wire surface is deliberately small:

```json
{"jsonrpc":"2.0","id":1,"method":"agent.ping","params":{}}
```

returns:

```json
{"jsonrpc":"2.0","id":1,"result":{"version":"0.1.0"}}
```

`agent.shutdown` is also accepted. One NDJSON frame is read at a time, stdout
is reserved for JSON-RPC, and malformed or unknown requests return standard
JSON-RPC errors without echoing request data. JSON syntax errors return
`-32700`; valid JSON with an invalid request shape returns `-32600`.

For `agent.ping` and `agent.shutdown`, `params` may be omitted or be the empty
object `{}`. `null`, arrays, and non-empty objects return `-32602`. Request IDs
are limited to strings and JSON integers, including negative integers; the
response preserves the original ID. Frames are read incrementally with a 1 MiB
limit. An oversized frame returns a parse error and ends the stdin loop. EOF
performs the same graceful shutdown path. For an explicit shutdown request, the
agent shutdown completes before its success response is written.

The library currently exposes `AgentConfig`, `AgentError`, `Agent`, the
single-consumer `AgentEventStream`, and `run_stdio`. The event stream has no
events yet by design. `data_dir` is parsed and validated but is not opened;
the Store implementation is crate-internal and uses
`<data_dir>/sessions/<session-id>/` with `session.json`, `manifest.json`, and
`conversation.log`. Each append is one durable JSON line containing one batch.
Store assumes one process per data directory and does not implement file locks.
Unknown append outcomes make that log object unusable. Agent Loop, Workspace,
Tools, Context, Policy, and Provider adapters belong to later phases. No
workspace, unsafe code, Factory/Repository layer, EventHub, plugin system, or
multi-client coordination is introduced. The offline process coverage is in
`tests/rpc_stdio.rs`; Store coverage is in the internal unit tests of
`src/store.rs`.
