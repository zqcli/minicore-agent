# MiniCore Agent

This repository is the Phase 1 RPC-first skeleton: one Rust package with a
library API and the `minicore-agent` stdio binary. It is verified against the
`minicore-runtime` `dev` HEAD
`7e85eaab18e273e43e03c50040b460f1b13f0ac9` through
`tests/runtime_api_compile.rs`. The runtime is a sibling path dependency and
is not modified here.

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
JSON-RPC errors without echoing request data.

The library currently exposes `AgentConfig`, `AgentError`, `Agent`, the
single-consumer `AgentEventStream`, and `run_stdio`. The event stream has no
events yet by design. `data_dir` is parsed and validated but is not opened;
Store, Agent Loop, Workspace, Tools, Context, Policy, and Provider adapters
belong to later phases. No workspace, unsafe code, Factory/Repository layer,
EventHub, plugin system, or multi-client coordination is introduced.
