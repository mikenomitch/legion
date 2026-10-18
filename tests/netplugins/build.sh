#!/usr/bin/env bash
# Build the test plugin modules for wasm32-wasip1 and copy them next to this
# script, where the plugin host tests and net_bench load them. The .wasm
# files are checked in, so `cargo test` needs no WASM toolchain; rerun this
# after changing a module's source.
#
#   rustup target add wasm32-wasip1
#   tests/netplugins/build.sh
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
for crate in conformance read-only-free-tier; do
    (cd "$here/$crate" && cargo build --release --target wasm32-wasip1 --quiet)
done
cp "$here/conformance/target/wasm32-wasip1/release/legion_netplugin_conformance.wasm" "$here/conformance.wasm"
cp "$here/read-only-free-tier/target/wasm32-wasip1/release/read_only_free_tier.wasm" "$here/read-only-free-tier.wasm"
sha256sum "$here"/*.wasm
