# Security Boundary

MiniCore Agent is a local execution component. Its safety model assumes a
cooperative local client and does not treat the Agent as a general-purpose
sandbox.

## Host Authority

The `bash` Tool can run commands with the operating-system authority of the
Agent process, including access to files, network resources, and inherited
permissions. `approval = "ask"` and `read_only` control Tool policy, not OS
capabilities. Use a container, VM, restricted account, or another OS boundary
when the model or command is not trusted.

The stdio RPC transport has no network authentication layer. Keep the process
and its pipes local to the intended client. Presentation responses/events are a
local-display permission: bounded command text, workspace paths, write bodies,
edit bodies, and patch input may be exposed to the trusted UI. Do not forward
those frames to a less-trusted service without applying an additional policy.

## Workspace And Store

`Workspace` canonicalizes its root and accepts workspace-relative Tool paths.
Absolute paths, parent traversal, NUL, invalid file types, and escape symlinks
are rejected; symlinks that resolve inside the root may be read under the
cooperative model. Atomic writes recheck parents and targets and report an
explicit unknown outcome if a post-rename directory sync fails.

These checks are not a defense against an adversarial process running as the
same user. Such a process can race path components or replace files between
checks and operations. Workspace reads are live observations, not filesystem
snapshots. The Store creates and protects one local `data_dir` boundary, but it
has no cross-process lock: run only one Agent process per data directory.

## Secrets, Logs, And Data

API key values are read from the environment variable named by `api_key_env`;
the value is not part of the TOML contract. Tracing is stderr-only and excludes
keys, authorization headers, provider URLs/bodies, prompts, reasoning text,
Tool arguments, and Bash commands/paths/content. Agent errors and redacted
`Debug` values expose stable classifications and bounded metadata, not secret
payloads.

The RPC and history views are different from logs: user text, Tool results, and
some bounded raw Tool streams are intentionally queryable data and may contain
secrets supplied by a user or model. Opaque provider reasoning is removed from
sanitized history views. Treat `data_dir`, `history.jsonl`, auxiliary Tool files,
and captured workspace content as sensitive local data; never commit or upload
it as a fixture.

The binary's `RUST_LOG` environment is combined with a target filter that
accepts only `minicore_agent` and `minicore_agent::*`; formatted diagnostics go
to stderr, never the RPC stdout stream. The bounded event channel is not an
audit log. Events may be dropped; use the authoritative query/persistence paths
when correctness matters.
