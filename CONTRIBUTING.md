# Contributing

MiniCore Agent is a Rust 2024 crate with a local stdio JSON-RPC service. Keep
changes focused, preserve the public API and wire contract unless a change
explicitly requires otherwise, and document user-visible behavior in the
current guides rather than in historical evidence records.

## Local Checks

The normal contributor checks are:

```bash
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --locked --all-targets
cargo test --locked --doc
cargo clippy --locked --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
```

The supported Rust floor is 1.85. Run the focused tests relevant to a change,
then the all-target suite. Do not use real provider credentials for routine
checks; Live/provider smoke tests remain explicitly opt-in where marked.

## CI Matrix

The workflow in `.github/workflows/ci.yml` is the source of truth:

- Quality on Ubuntu with stable Rust: rustfmt, all-target Clippy with warnings
  denied, and warning-denied rustdoc.
- Stable all-target tests on Ubuntu, macOS, and Windows.
- All-target tests on Ubuntu with Rust 1.85.0.

Platform behavior that was only cross-compiled, provider behavior that was not
run, and native UI behavior that was not exercised must remain described as
unverified. A remote verification run may be recorded for a particular review
or release record; that evidence is scoped to the run and is not a replacement
for the hosted CI matrix. See the [verification index](docs/verification/README.md)
for the distinction.

## Data And Secrets

Never commit API keys, credential-bearing configuration, local Stores,
conversation history, generated binaries, private provider responses, or user
workspace data. Keep secret values in the environment variable named by
`api_key_env`; do not put the value in TOML, examples, tests, logs, or docs.
Use synthetic fixtures and loopback providers for tests.

## Version And Documentation Rules

- Change the package version in `Cargo.toml` and the `minicore-agent` package
  entry in `Cargo.lock` together; do not run a broad dependency update for a
  version-only change.
- Treat the Runtime Git revision, `RPC_PROTOCOL_VERSION`, storage format
  versions, public signatures, and RPC/event schemas as independent contracts.
  Change them only with an explicit compatibility decision and coverage.
- Current behavior belongs in `README.md` or the focused guides under `docs/`.
  Release notes belong in `docs/releases/` or `CHANGELOG.md`.
- Keep historical specifications and verification records factual. Do not
  rewrite old counts, source revisions, artifact hashes, or limitations to make
  them match a later release.
- Prefer concise rustdoc examples. Examples that would open files, contact a
  provider, or mutate a workspace should use `no_run` and must not claim that
  execution was performed.

See the [documentation index](docs/README.md) and [RPC contract](docs/rpc.md)
for the maintained entry points.
