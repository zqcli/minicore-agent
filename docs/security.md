# Security Boundary

MiniCore Agent is a local execution component. Its safety model assumes a
cooperative local client and does not treat the Agent as a general-purpose
sandbox.

## Host Authority

The `bash` Tool can run commands with the operating-system authority of the
Agent process, including access to files, network resources, and inherited
permissions. `approval = "ask"` and `read_only` control Tool policy, not OS
capabilities. Use a container, VM, restricted account, or another OS boundary
when the model or command is not trusted. On Unix the Tool runs `/bin/bash -c`
when available, otherwise `/bin/sh -c`, never a login shell. It inherits the
Agent environment, except configured credential variables are removed and
`MINICORE_AGENT=1` is set. Profiles are not re-sourced by a login shell. Windows
continues to use PowerShell with `-NoProfile`.

Unix cancellation sends SIGTERM to the process group and allows a 500 ms grace
while draining both output pipes, then sends SIGKILL and reaps it. Model-visible
output keeps head and tail with an omitted-bytes marker; stdout and stderr share
one output budget. If the Agent would be PID 1 in a container, run an init process
(`--init` or tini) to reap orphaned descendants. Otherwise orphaned children can
remain zombies and prevent confirmation that the process group is gone.

Process-group signals use the numeric leader PID. There is a theoretical PID-reuse
window between reaping the leader and a later group signal: if that number is
reused as another process group's ID, the signal could target the unrelated group.
Process-group cleanup is not a defense against a hostile same-user process.

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
explicit unknown outcome if a post-rename directory sync fails. Replacing an
existing file preserves its permission bits, including private and executable
modes; ownership and extended attributes are not preserved. These checks apply
to `write`, `edit`, and `apply_patch`.

Atomic commits run on the blocking pool, not on an async runtime worker. Once a
commit is started it finishes and records its file-change observation even if the
Tool future is dropped. Cancellation or timeout may therefore return before the
file is replaced; neither means the mutation was rolled back. Path preflight and
source reads remain cancellable before the commit starts.

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

A non-loopback `http://` model `base_url` sends the bearer token and request/response
content in plaintext. It remains allowed for trusted LAN gateways, but the Agent
logs one warning per such model when constructing the catalog at startup or reload.
The warning identifies only the configured model ID, never the key, host, or URL
path. Prefer HTTPS outside a trusted local network; loopback HTTP does not receive
this warning. Redirects remain disabled.

The RPC and history views are different from logs: user text, Tool results, and
some bounded raw Tool streams are intentionally queryable data and may contain
secrets supplied by a user or model. Opaque provider reasoning is removed from
sanitized history views, read/export responses, and summary prose. Validated
provider replay (including encrypted reasoning) is retained privately in the
existing JSONL history, bound to provider endpoint and actual model; it is not
a secret store and is not reinterpreted as tool authority. Treat `data_dir`, `history.jsonl`, auxiliary Tool files,
and captured workspace content as sensitive local data; never commit or upload
it as a fixture.

The binary's `RUST_LOG` environment is combined with a target filter that
accepts only `minicore_agent` and `minicore_agent::*`; formatted diagnostics go
to stderr, never the RPC stdout stream. The bounded event channel is not an
audit log. Events may be dropped; use the authoritative query/persistence paths
when correctness matters.
