#!/usr/bin/env bash
# Build the shipping-profile legion binary and stage it in the rig's S3
# bucket, where every instance (role-authenticated) pulls it from.
# Records the git revision + sha256 next to it for the fairness record.
#
# Usage: build-and-upload.sh [--skip-build] [--force-plugins]
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

# `dist` is the shipping profile (fat LTO, one codegen unit) and takes
# ~25 minutes to link on this box. LEGION_BUILD_PROFILE=distfast swaps in
# thin LTO across 16 units for roughly a third of the time, for sessions
# where the measurement window is shorter than the build. The fairness
# record carries the profile, so a cell can never silently claim to be a
# `dist` result when it is not.
BUILD_PROFILE="${LEGION_BUILD_PROFILE:-dist}"
BIN="$REPO_ROOT/target/$BUILD_PROFILE/legion"
# The rig's native job submitter (benchmarks/loadgen). Built from the same
# tree, same profile, same commit as the CLI — a submitter built from a
# different revision than the manager under test would be a fairness-record
# hole. Feature-gated so it only exists when asked for.
LOADGEN_BIN="$REPO_ROOT/target/$BUILD_PROFILE/legion-loadgen"

SKIP_BUILD=false FORCE_PLUGINS=false
for arg in "$@"; do
    case "$arg" in
    --skip-build) SKIP_BUILD=true ;;
    --force-plugins) FORCE_PLUGINS=true ;;
    *)
        echo "unknown arg $arg (--skip-build|--force-plugins)" >&2
        exit 1
        ;;
    esac
done

# --- Do the plugins actually need rebuilding? --------------------------
#
# They almost never do, and rebuilding them is most of this script's wall
# clock. The repo's build.rs gives every plugin its own CARGO_TARGET_DIR,
# so each of the six compiles the same ~470 dependency crates into its
# own tree: ~10.9 GB of build output and the dominant share of the ~17
# minute clean build that every campaign pays. build.rs is not ours to
# change (it is the repo root's, and the rig must build the shipping
# binary the same way anyone else would), so the lever we have is not
# asking for the work when the answer cannot have changed.
#
# The fingerprint is the content of plugins/ plus the compiler version —
# the two inputs that decide what the dylibs contain. Anything else
# changing (src/, proto/, Cargo.lock at the workspace level) does not
# affect a plugin dylib: plugins are separate crates, loaded at runtime
# over a stable ABI, and the manager validates them by manifest. If that
# ever stops being true, --force-plugins is the escape hatch, and a
# changed toolchain invalidates the cache on its own.
plugin_fingerprint() {
    {
        rustc -V 2>/dev/null || echo "rustc unknown"
        # Sorted so the hash is stable across filesystems; target/ trees
        # excluded because they are build OUTPUT, not input.
        find "$REPO_ROOT/plugins" -type f -not -path '*/target/*' -print0 |
            sort -z | xargs -0 sha256sum
    } | sha256sum | cut -d' ' -f1
}

PLUGIN_FP=$(plugin_fingerprint)
PLUGINS_CACHED=false
if ! $FORCE_PLUGINS; then
    REMOTE_FP=$(aws s3 cp --only-show-errors --region "$REGION" \
        "s3://$BUCKET/bin/legion-plugins.meta.json" - 2>/dev/null |
        jq -r '.plugins_fingerprint // empty' 2>/dev/null || true)
    # The fingerprint alone is not enough: it says the SOURCES match, not
    # that the bundle those sources produced is still in the bucket.
    if [[ -n "$REMOTE_FP" && "$REMOTE_FP" == "$PLUGIN_FP" ]] &&
        aws s3api head-object --region "$REGION" --bucket "$BUCKET" \
            --key bin/legion-plugins.tar.gz >/dev/null 2>&1; then
        PLUGINS_CACHED=true
    fi
fi

if ! $SKIP_BUILD; then
    if $PLUGINS_CACHED; then
        log "plugins unchanged (fingerprint $PLUGIN_FP) — reusing the staged bundle"
        log "building legion + legion-loadgen (dist profile, no plugins; this link is slow by design)"
        (cd "$REPO_ROOT" && LEGION_SKIP_PLUGIN_BUILD=1 cargo build --profile "$BUILD_PROFILE" --features loadgen)
    else
        log "building legion + legion-loadgen (dist profile + plugins; this link is slow by design)"
        (cd "$REPO_ROOT" && LEGION_BUILD_PLUGINS=1 cargo build --profile "$BUILD_PROFILE" --features loadgen)
    fi
fi
[[ -x "$BIN" ]] || {
    echo "no dist binary at $BIN — run without --skip-build" >&2
    exit 1
}
[[ -x "$LOADGEN_BIN" ]] || {
    echo "no legion-loadgen at $LOADGEN_BIN — build with --features loadgen" >&2
    exit 1
}
REV=$(git -C "$REPO_ROOT" rev-parse HEAD)
SHA=$(sha256sum "$BIN" | cut -d' ' -f1)

LOADGEN_SHA=$(sha256sum "$LOADGEN_BIN" | cut -d' ' -f1)

log "uploading legion ($REV) to s3://$BUCKET/bin/"
aws s3 cp --only-show-errors --region "$REGION" "$BIN" "s3://$BUCKET/bin/legion"
log "uploading legion-loadgen ($REV) to s3://$BUCKET/bin/"
aws s3 cp --only-show-errors --region "$REGION" "$LOADGEN_BIN" "s3://$BUCKET/bin/legion-loadgen"

# The manager validates task runtimes against their plugin dylibs, so
# the campaign boxes need the plugins tree in the canonical deployed
# layout: <root>/<kind>/<name>/{plugin.json,target/release/lib<name>.so},
# resolved via LEGION_PLUGIN_DIR. The workspace build (build.rs with
# LEGION_BUILD_PLUGINS=1) lands dylibs under target/plugin-build/, so
# stage a bundle that flattens either build layout into the canonical one.
if $PLUGINS_CACHED; then
    log "plugins bundle already staged for this fingerprint — not re-uploading"
else
    log "staging plugins bundle"
    PLUGIN_STAGE=$(mktemp -d)
    trap 'rm -rf "$PLUGIN_STAGE"' EXIT
    found_any=false
    for manifest in "$REPO_ROOT"/plugins/*/*/plugin.json; do
        plugin_dir=$(dirname "$manifest")
        plugin=$(basename "$plugin_dir")
        kind=$(basename "$(dirname "$plugin_dir")")
        dylib=""
        for candidate in \
            "$plugin_dir/target/release/lib$plugin.so" \
            "$REPO_ROOT/target/plugin-build/$kind/$plugin/release/lib$plugin.so"; do
            [[ -f "$candidate" ]] && dylib="$candidate" && break
        done
        [[ -n "$dylib" ]] || {
            log "  skipping $kind/$plugin (no built dylib)"
            continue
        }
        mkdir -p "$PLUGIN_STAGE/plugins/$kind/$plugin/target/release"
        cp "$manifest" "$PLUGIN_STAGE/plugins/$kind/$plugin/plugin.json"
        cp "$dylib" "$PLUGIN_STAGE/plugins/$kind/$plugin/target/release/lib$plugin.so"
        found_any=true
    done
    $found_any || {
        echo "no built plugin dylibs found — build with LEGION_BUILD_PLUGINS=1" >&2
        exit 1
    }
    tar -C "$PLUGIN_STAGE" -czf - plugins |
        aws s3 cp --only-show-errors --region "$REGION" - "s3://$BUCKET/bin/legion-plugins.tar.gz"

    # The bundle's cache key, written only after the bundle it describes
    # has landed. Written second on purpose: a fingerprint in the bucket
    # without a bundle behind it would let the next run skip a build it
    # actually needs.
    printf '{"plugins_fingerprint":"%s","git_rev":"%s","staged_at":"%s"}\n' \
        "$PLUGIN_FP" "$REV" "$(date -u +%FT%TZ)" |
        aws s3 cp --only-show-errors --region "$REGION" - "s3://$BUCKET/bin/legion-plugins.meta.json"
fi

printf '{"git_rev":"%s","sha256":"%s","loadgen_sha256":"%s","built_at":"%s"}\n' \
    "$REV" "$SHA" "$LOADGEN_SHA" "$(date -u +%FT%TZ)" |
    aws s3 cp --only-show-errors --region "$REGION" - "s3://$BUCKET/bin/legion.meta.json"
log "uploaded (sha256 $SHA, loadgen $LOADGEN_SHA)"

# Install the CLI on the loadgen box now that it exists in S3.
#
# install-loadgen.sh also does this, but it runs inside bringup.sh —
# before this script has ever run, when the bucket holds no binary. It
# warns and continues, which is right (the bucket is created by the same
# bringup), but it leaves the loadgen box without a `legion` on PATH.
# Every later step needs it: start-sim-fleet.sh mints join tokens with
# it, run-step.sh queries node counts with it, and both fail with a bare
# `legion: command not found` a long way from this cause.
log "installing the legion CLI + native submitter on the loadgen box"
rsh "$LOADGEN_ADDR" "
    sudo aws s3 cp --only-show-errors --region $REGION s3://$BUCKET/bin/legion /opt/legion/bin/legion &&
    sudo chmod +x /opt/legion/bin/legion &&
    sudo ln -sf /opt/legion/bin/legion /usr/local/bin/legion &&
    sudo aws s3 cp --only-show-errors --region $REGION s3://$BUCKET/bin/legion-loadgen /opt/legion/bin/legion-loadgen &&
    sudo chmod +x /opt/legion/bin/legion-loadgen &&
    sudo ln -sf /opt/legion/bin/legion-loadgen /usr/local/bin/legion-loadgen
" >/dev/null || {
    echo "failed to install the legion binaries on the loadgen box ($LOADGEN_ADDR)" >&2
    exit 1
}
rsh "$LOADGEN_ADDR" "command -v legion >/dev/null && command -v legion-loadgen >/dev/null" || {
    echo "legion/legion-loadgen still not on PATH on the loadgen box" >&2
    exit 1
}
log "legion CLI + legion-loadgen ready on the loadgen box"
