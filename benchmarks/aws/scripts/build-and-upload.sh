#!/usr/bin/env bash
# Build the shipping-profile legion binary and stage it in the rig's S3
# bucket, where every instance (role-authenticated) pulls it from.
# Records the git revision + sha256 next to it for the fairness record.
#
# Usage: build-and-upload.sh [--skip-build]
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

BIN="$REPO_ROOT/target/dist/legion"

if [[ "${1:-}" != "--skip-build" ]]; then
    log "building legion (dist profile + plugins; this link is slow by design)"
    (cd "$REPO_ROOT" && LEGION_BUILD_PLUGINS=1 cargo build --profile dist)
fi
[[ -x "$BIN" ]] || {
    echo "no dist binary at $BIN — run without --skip-build" >&2
    exit 1
}
REV=$(git -C "$REPO_ROOT" rev-parse HEAD)
SHA=$(sha256sum "$BIN" | cut -d' ' -f1)

log "uploading legion ($REV) to s3://$BUCKET/bin/"
aws s3 cp --region "$REGION" "$BIN" "s3://$BUCKET/bin/legion"

# The manager validates task runtimes against their plugin dylibs, so
# the campaign boxes need the plugins tree in the canonical deployed
# layout: <root>/<kind>/<name>/{plugin.json,target/release/lib<name>.so},
# resolved via LEGION_PLUGIN_DIR. The workspace build (build.rs with
# LEGION_BUILD_PLUGINS=1) lands dylibs under target/plugin-build/, so
# stage a bundle that flattens either build layout into the canonical one.
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
    aws s3 cp --region "$REGION" - "s3://$BUCKET/bin/legion-plugins.tar.gz"

printf '{"git_rev":"%s","sha256":"%s","built_at":"%s"}\n' \
    "$REV" "$SHA" "$(date -u +%FT%TZ)" |
    aws s3 cp --region "$REGION" - "s3://$BUCKET/bin/legion.meta.json"
log "uploaded (sha256 $SHA)"

# Install the CLI on the loadgen box now that it exists in S3.
#
# install-loadgen.sh also does this, but it runs inside bringup.sh —
# before this script has ever run, when the bucket holds no binary. It
# warns and continues, which is right (the bucket is created by the same
# bringup), but it leaves the loadgen box without a `legion` on PATH.
# Every later step needs it: start-sim-fleet.sh mints join tokens with
# it, run-step.sh queries node counts with it, and both fail with a bare
# `legion: command not found` a long way from this cause.
log "installing the legion CLI on the loadgen box"
rsh "$LOADGEN_ADDR" "
    sudo aws s3 cp --region $REGION s3://$BUCKET/bin/legion /opt/legion/bin/legion &&
    sudo chmod +x /opt/legion/bin/legion &&
    sudo ln -sf /opt/legion/bin/legion /usr/local/bin/legion
" >/dev/null || {
    echo "failed to install the legion CLI on the loadgen box ($LOADGEN_ADDR)" >&2
    exit 1
}
rsh "$LOADGEN_ADDR" "command -v legion >/dev/null" || {
    echo "legion still not on PATH on the loadgen box" >&2
    exit 1
}
log "legion CLI ready on the loadgen box"
