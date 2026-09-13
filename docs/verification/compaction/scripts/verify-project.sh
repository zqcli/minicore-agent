#!/usr/bin/env bash
set -euo pipefail
[[ "$(uname -s)" == Linux ]] || exit 2
project=${1:?project}
stage=${2:?stage}
[[ "$project" == minicore-agent || "$project" == minicore-tui ]] || exit 2
[[ "$stage" =~ ^[a-z0-9-]+$ ]] || exit 2
root=/root/minicore-compaction.j5Jaqn
cache=/root/minicore-tui-027-RXdxEP
export PATH=/root/.cargo/bin:$PATH
export CARGO_TARGET_DIR="$cache/target-linux"
cd "$root/source/$project"
run() {
  local label=$1
  shift
  local log="$root/logs/$stage-$label.log" result=0
  printf 'cwd=%s\ncommand=' "$PWD" > "$log"
  printf '%q ' "$@" >> "$log"
  printf '\n' >> "$log"
  "$@" >> "$log" 2>&1 || result=$?
  printf '\nexit=%s\n' "$result" >> "$log"
  if [[ "$result" != 0 ]]; then tail -n 75 "$log"; exit "$result"; fi
}
run format rustup run stable cargo fmt --all
run stable rustup run stable cargo test -j4 --locked --offline --all-targets
run msrv env CARGO_TARGET_DIR="$cache/target-msrv" rustup run 1.85.0 cargo test -j4 --locked --offline --all-targets
run fmt-check rustup run stable cargo fmt --all -- --check
run clippy rustup run stable cargo clippy -j4 --locked --offline --all-targets -- -D warnings
run doc env RUSTDOCFLAGS=-D\ warnings rustup run stable cargo doc -j4 --locked --offline --no-deps
run build rustup run stable cargo build -j4 --locked --offline
cd "$root/source/minicore-tui"
export MINICORE_AGENT_BIN="$cache/target-linux/debug/minicore-agent"
run e2e-stable rustup run stable cargo test -j4 --locked --offline --test agent_e2e -- --ignored --test-threads=1
run e2e-msrv env CARGO_TARGET_DIR="$cache/target-msrv" rustup run 1.85.0 cargo test -j4 --locked --offline --test agent_e2e -- --ignored --test-threads=1
printf 'PARENT_COMPACTION_VERIFIED project=%s stage=%s\n' "$project" "$stage"
