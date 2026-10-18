# MiniCore Agent

[![CI](https://github.com/zqcli/minicore-agent/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/zqcli/minicore-agent/actions/workflows/ci.yml)

MiniCore Agent is a local, RPC-first coding-agent backend written in Rust. It
manages persistent Sessions over rooted Workspaces, runs model/tool turns on
[MiniCore Runtime](https://github.com/zqcli/minicore-runtime), and exposes
everything to a client UI through newline-delimited JSON-RPC on stdio. It can
also be embedded directly as a Rust library.

| | |
|---|---|
| Version | 0.7.0 |
| Rust | edition 2024, MSRV 1.85 |
| Runtime | `minicore-runtime` 0.7.0, pinned to `86fba7893a09a6cdab76c52da07f5fc93c307a90` |
| RPC protocol | 1 (33 methods) |
| Provider | OpenAI Responses API and compatible endpoints |
| License | MIT OR Apache-2.0 |

## Features

- **Sessions.** Several Sessions can be loaded at once, each with one active
  turn at a time. History is an append-only JSONL file and survives restarts.
  Turns can be steered, cancelled, waited on, and inspected after the fact.
- **Tools.** `read`, `write`, `edit`, `apply_patch`, and `bash`, with approval
  modes `ask`, `auto`, and `read_only`. Interactive approvals are answered
  over RPC.
- **Workspace queries.** Bounded file listing, search, file reads, Git status,
  and per-turn change tracking with diffs. Paths are confined to the Workspace
  root.
- **Context management.** Automatic and manual compaction into source-bound
  summaries, plus a one-time compact-and-retry when the provider rejects a
  request for context length.
- **Provider support.** Streaming Responses with reasoning, retries honoring
  `Retry-After`, and durable provider replay across tool rounds and restarts.
- **Bounded and redacted.** Queues, reads, tool output, and history scans have
  explicit limits. Keys, prompts, and provider bodies stay out of logs and
  errors.

## How It Fits Together

```text
client UI ──stdio JSON-RPC──► minicore-agent
                               ├─ Sessions ──► minicore-runtime AgentLoop (one per turn)
                               ├─ Models ────► OpenAI Responses provider
                               ├─ Tools ─────► Workspace (rooted file access, Bash)
                               └─ Store ─────► <data_dir>/sessions/<session_id>/
```

Runtime runs a single loop of model requests and tool calls. The Agent owns
everything around that loop: Sessions, persistence, providers, tools,
compaction, and the RPC surface. See [Architecture](docs/architecture.md).

## Quickstart

You need Rust 1.85 or newer and an API key for an OpenAI Responses-compatible
endpoint. Cargo fetches the pinned Runtime revision itself; no sibling checkout
is required.

```bash
cargo build --locked --release
cp example.agent.toml agent.toml   # set models.main.model to a real model ID
export OPENAI_API_KEY=...           # the variable named by api_key_env
./target/release/minicore-agent --config ./agent.toml --stdio
```

The service reads one JSON-RPC request per line on stdin and writes responses
and `agent.event` notifications to stdout. Diagnostics go to stderr.

```json
{"jsonrpc":"2.0","id":1,"method":"agent.ping"}
```

The binary accepts only `--version` and `--config <path> --stdio`. It exits
after an orderly shutdown on stdin EOF, Ctrl-C, SIGTERM (Unix), or
`agent.shutdown`.

> [!WARNING]
> `example.agent.toml` uses `approval = "auto"`, which runs every enabled tool,
> including Bash, without asking. Use it only in a trusted environment, or
> switch to `ask`.

See [Configuration](docs/configuration.md) for every TOML field, prompt files,
reload, and custom TLS roots (`SSL_CERT_FILE`).

## RPC Surface

| Area | Methods |
|---|---|
| Agent | `agent.ping`, `agent.reload`, `agent.shutdown` |
| Discovery | `profile.list`, `model.list` |
| Sessions | `session.create`, `session.open`, `session.close`, `session.delete`, `session.list`, `session.state`, `session.update`, `session.rename`, `session.context`, `session.compact`, `session.compact.cancel` |
| Turns | `turn.send`, `turn.steer`, `turn.cancel`, `turn.wait`, `turn.result` |
| Interaction | `interaction.answer` |
| Read and review | `session.history`, `session.read`, `session.presentation`, `tool.read`, `tool.output`, `workspace.files`, `workspace.search`, `workspace.read`, `workspace.status`, `changes.list`, `changes.diff` |

[docs/rpc.md](docs/rpc.md) is the normative wire contract: parameters, results,
events, ordering, limits, and error codes.

## Using It As A Library

```toml
[dependencies]
# Pin a `rev` or release tag for reproducible builds.
minicore-agent = { git = "https://github.com/zqcli/minicore-agent", branch = "dev" }
```

```rust
use minicore_agent::{Agent, CreateSession, SendMessage};

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut agent = Agent::open_file("agent.toml").await?;
    let session = agent
        .create_session(CreateSession {
            workspace: "/path/to/project".into(),
            profile: String::new(), // empty selects default_profile
            model: None,
            reasoning: None,
            title: None,
        })
        .await?;
    let turn = agent
        .send(SendMessage {
            session_id: session.session_id,
            text: "Summarize this repository.".into(),
        })
        .await?;
    let result = agent.wait_turn(turn).await?;
    println!("{:?} ({:?})", result.report.outcome, result.persistence);
    agent.shutdown().await?;
    Ok(())
}
```

`Agent::open` builds an Agent from an in-memory `AgentConfig` without a reload
source. `Agent::take_events` returns the live event stream, and `run_stdio`
serves an Agent over stdin/stdout. Call `Agent::shutdown` to join outstanding
work; dropping an Agent does not wait for it. Generate the API docs with
`cargo doc --locked --no-deps --open`.

## Data And Persistence

Each Session lives under `<data_dir>/sessions/<session_id>/`: `session.json`
for metadata, `history.jsonl` for the authoritative history, `summary.json` for
the derived compaction snapshot, and `tools/` for auxiliary tool records.

- A turn result reports `persistence: persisted` only after its history append
  is synced to disk. A failed append leaves the Session usable unless the
  outcome is uncertain, in which case the Session is blocked until reopened.
- Events are best-effort. `turn.wait`, `turn.result`, and history queries are
  authoritative.
- Run one Agent process per `data_dir`; the Store takes no cross-process lock.
- Session data from v0.2 is not migrated.

## Trust Boundary

MiniCore Agent is a local backend, not a sandbox. Bash runs with the Agent
process's authority, and Workspace path checks do not defend against another
process racing the filesystem. Isolate untrusted models or commands with OS or
container mechanisms. Presentation data intentionally shows local paths and
commands to the trusted UI, so add your own policy before forwarding RPC
elsewhere. See [Security](docs/security.md).

## Compatibility

The RPC protocol and the storage formats are versioned independently of the
package. 0.7.0 keeps protocol 1 and the existing on-disk formats, but changes
some client-visible behavior: the shutdown cancellation reason, settlement
finishing after `turn.wait`, a separate lifecycle event queue, path-based scan
cursors, and credential checks. The [changelog](CHANGELOG.md) lists every change.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
```

CI checks formatting, Clippy, and rustdoc on stable Rust, runs the tests on
Ubuntu, macOS, and Windows, and runs the tests again with Rust 1.85.0. Unit tests live in `<module>/tests.rs` files.
Integration tests are the `it` target plus `rpc_soak` (see
[tests/README.md](tests/README.md)). Tests use loopback providers only; never
use real credentials. See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution
rules and [AGENTS.md](AGENTS.md) for coding-agent guidance.

## Documentation

| Guide | Contents |
|---|---|
| [Configuration](docs/configuration.md) | TOML fields, secrets, prompt files, reload, TLS |
| [RPC contract](docs/rpc.md) | Normative JSON-RPC methods, events, and errors |
| [Architecture](docs/architecture.md) | Ownership of Agent, Session, Runtime, Tools, and Store |
| [Context and compaction](docs/context.md) | Summaries, triggers, and emergency recovery |
| [Provider replay](docs/provider-replay.md) | Durable provider output and redaction |
| [Security](docs/security.md) | Trust assumptions, Workspace limits, and redaction |
| [Changelog](CHANGELOG.md) | Release notes and compatibility |

[docs/README.md](docs/README.md) also indexes historical verification records
and archived specifications. Those describe past runs, not the current release.

## License

Licensed under `MIT OR Apache-2.0`, as declared in `Cargo.toml`. The compaction
prompts in `src/compaction/prompts/` are adapted from
[Pi](https://github.com/earendil-works/pi) under the MIT license; see
[their notice](src/compaction/prompts/LICENSE).
