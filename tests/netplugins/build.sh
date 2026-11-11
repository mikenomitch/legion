#!/usr/bin/env bash
# Build the test plugin modules for wasm32-wasip1 and copy them next to this
# script, where the plugin host tests and net_bench load them. The .wasm
# files are checked in, so `cargo test` needs no WASM toolchain; rerun this
# after changing a module's source, and commit the result.
#
#   rustup toolchain install 1.97.0 --target wasm32-wasip1
#   tests/netplugins/build.sh           # rebuild them
#   tests/netplugins/build.sh --check   # compare only
#
# Both need rustc 1.97.0 active (prefix RUSTUP_TOOLCHAIN=1.97.0 when it is
# not your default).
#
# CI runs --check, so the build is pinned to make it byte-for-byte
# reproducible:
#   - the toolchain (NETPLUGIN_TOOLCHAIN, default below): a new rustc emits
#     different code;
#   - each module's Cargo.lock, checked in beside its Cargo.toml;
#   - the Cargo home in panic-location strings, remapped to /cargo, so the
#     bytes do not depend on whose machine built them.
# A toolchain bump is a rebuild and a commit of both modules, and of the
# read-only-free-tier checksum in configs/netplugins/read-only-free-tier.ncl
# and docs/network-plugins.md.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
toolchain="${NETPLUGIN_TOOLCHAIN:-1.97.0}"
cargo_home="${CARGO_HOME:-$HOME/.cargo}"
check=false
[ "${1:-}" = "--check" ] && check=true

found="$(rustc --version)"
case "$found" in
"rustc $toolchain "*) ;;
*)
    echo "tests/netplugins/build.sh needs rustc $toolchain, found: $found" >&2
    echo "  rustup toolchain install $toolchain --target wasm32-wasip1, then rerun with RUSTUP_TOOLCHAIN=$toolchain" >&2
    exit 2
    ;;
esac

target_dir="$(mktemp -d)"
trap 'rm -rf "$target_dir"' EXIT

build() { # build <crate-dir> <artifact-name> <checked-in-name>
    (cd "$here/$1" &&
        RUSTFLAGS="--remap-path-prefix=$cargo_home=/cargo" CARGO_TARGET_DIR="$target_dir" \
            cargo build --release --locked --target wasm32-wasip1 --quiet) || {
        echo "tests/netplugins/$1 failed to build" >&2
        return 1
    }
    local built="$target_dir/wasm32-wasip1/release/$2"
    if $check; then
        if ! cmp -s "$built" "$here/$3"; then
            echo "tests/netplugins/$3 does not match its source: run tests/netplugins/build.sh and commit the result" >&2
            return 1
        fi
        echo "tests/netplugins/$3 is current"
    else
        cp "$built" "$here/$3"
    fi
}

stale=0
build conformance legion_netplugin_conformance.wasm conformance.wasm || stale=1
build read-only-free-tier read_only_free_tier.wasm read-only-free-tier.wasm || stale=1
$check || sha256sum "$here"/*.wasm
exit "$stale"
