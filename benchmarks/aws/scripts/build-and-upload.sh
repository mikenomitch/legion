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
[[ -f "$REPO_ROOT/plugins/tasks/noop/target/release/libnoop.so" ]] || {
    echo "noop plugin dylib missing — build with LEGION_BUILD_PLUGINS=1" >&2
    exit 1
}

REV=$(git -C "$REPO_ROOT" rev-parse HEAD)
SHA=$(sha256sum "$BIN" | cut -d' ' -f1)

log "uploading legion ($REV) to s3://$BUCKET/bin/"
aws s3 cp --region "$REGION" "$BIN" "s3://$BUCKET/bin/legion"

# The manager validates task runtimes against their plugin dylibs, so
# the campaign boxes need the plugins tree (layout:
# <root>/<kind>/<name>/{plugin.json,target/release/lib<name>.so},
# resolved via LEGION_PLUGIN_DIR). Ship manifests + built dylibs only.
log "uploading plugins bundle"
(cd "$REPO_ROOT" &&
    find plugins -name plugin.json -o -path '*/target/release/*.so' -o -path '*/target/release/*.dylib' |
    tar -czf - -T -) |
    aws s3 cp --region "$REGION" - "s3://$BUCKET/bin/legion-plugins.tar.gz"

printf '{"git_rev":"%s","sha256":"%s","built_at":"%s"}\n' \
    "$REV" "$SHA" "$(date -u +%FT%TZ)" |
    aws s3 cp --region "$REGION" - "s3://$BUCKET/bin/legion.meta.json"
log "uploaded (sha256 $SHA)"
