# Documentation Index

This index separates current product guidance from historical records and
verification evidence.

## Current Guidance

- [README and quickstart](../README.md): package identity, build/run commands,
  API orientation, and trust boundary.
- [Configuration](configuration.md): TOML fields, provider secrets, path bases,
  prompt files, reload, and Session updates.
- [Architecture](architecture.md): Agent, Session, Runtime, Tool, Store, and
  query ownership.
- [Provider replay](provider-replay.md): durable output, identity, redaction, and bounds.
- [Context](context.md): source-bound request-boundary and post-turn/manual summaries,
  bounded emergency recovery, and
  context estimates/errors.
- [Security](security.md): local trust assumptions, Workspace limits, redaction,
  stdout, and Store scope.
- [RPC contract](rpc.md): the normative NDJSON wire contract and all 33 methods.
- [Changelog](../CHANGELOG.md): current candidate changes and compatibility notes.
- [Contributing](../CONTRIBUTING.md): checks, CI boundaries, and documentation rules.

These guides describe the current source package **0.7.4**, Rust 2024 on
current stable, Runtime `0.7.4` (tag `v0.7.4`), and RPC protocol version `1`.
A package version change does not by itself change the RPC protocol or storage
formats.

## Evidence And History

- [Verification index](verification/README.md): historical evidence plus the
  centralized 0.5.0 freeze record and its measured status.
- [0.5.0 freeze record](verification/0.5.0.md): historical source, compatibility,
  static checks, and measured remote gate results.
- [Archive index](archive/README.md): historical context, plans, and tracked
  specifications marked not current.
- [0.3.3 release notes](releases/0.3.3.md) and [0.3.2 notes](releases/0.3.2.md):
  historical release records whose versioned facts are preserved.

Do not use a historical count, artifact hash, source revision, or phase status
as a current implementation claim. Verification artifacts remain at their
original paths and are not part of the current documentation contract.
