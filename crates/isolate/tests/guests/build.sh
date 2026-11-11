#!/usr/bin/env bash
# Rebuild every test guest under tests/guests/ into tests/fixtures/.
#
# Each guest is a directory with its own Cargo.toml (not a workspace member;
# it has an empty [workspace] table) whose single target is named after the
# directory: tests/guests/<name>/ -> tests/fixtures/<name>.wasm. A command
# is a binary; a reactor (a component that exports interfaces instead of
# `wasi:cli/run`) is a `cdylib` library. Guests build for wasm32-wasip2,
# whose linker emits a component directly. `wit-deps/` holds WIT shared by
# the guests (it has no Cargo.toml). CI has no wasm
# target, so the fixtures are checked in; run this after changing a guest.
#
#   crates/isolate/tests/guests/build.sh          # every guest
#   crates/isolate/tests/guests/build.sh fs       # just the named ones
#
# Needs: rustup target add wasm32-wasip2
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fixtures="$(cd "$here/../fixtures" && pwd)"
repo="$(cd "$here/../../../.." && pwd)"
target_dir="${CARGO_TARGET_DIR:-$repo/target}/isolate-guests"
triple=wasm32-wasip2

if [ "$#" -gt 0 ]; then
    guests=("$@")
else
    guests=()
    for manifest in "$here"/*/Cargo.toml; do
        guests+=("$(basename "$(dirname "$manifest")")")
    done
fi

for name in "${guests[@]}"; do
    manifest="$here/$name/Cargo.toml"
    if [ ! -f "$manifest" ]; then
        echo "no guest at $here/$name" >&2
        exit 1
    fi
    echo "building $name"
    if grep -q 'cdylib' "$manifest"; then
        kind=(--lib)
    else
        kind=(--bin "$name")
    fi
    cargo build --release --locked --manifest-path "$manifest" \
        --target "$triple" --target-dir "$target_dir/$name" "${kind[@]}"
    cp "$target_dir/$name/$triple/release/$name.wasm" "$fixtures/$name.wasm"
    echo "  -> tests/fixtures/$name.wasm ($(wc -c <"$fixtures/$name.wasm") bytes)"
done
