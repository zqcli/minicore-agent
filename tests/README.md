# Integration tests

`it/main.rs` is the integration target. Each former integration binary is a
module, so run a suite with, for example:

```sh
cargo test --locked --test it openai_rpc_process::
cargo test --locked --test it startup_errors::
cargo test --locked --test rpc_soak
```

The standalone `rpc_soak` target keeps the longer repeated-turn scenarios
separate. `support/` contains the shared HTTP mock, RPC subprocess, temporary
directory fixture, configuration and response helpers. Both targets load it as
one module tree (`tests/it/main.rs` through a single `#[path]`), without
dead-code allowances.

The stdio framing suite retains its raw-pipe driver to test malformed input,
exact wire IDs, EOF and oversized frames. Startup rejection tests keep raw TOML
fixtures so invalid configuration is not normalized by the shared serializer.
The other process suites use the shared structured configuration helpers.

Historical verification logs retain their original binary names and counts.
