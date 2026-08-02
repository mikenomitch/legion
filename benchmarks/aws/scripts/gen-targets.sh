#!/usr/bin/env bash
# Regenerate the file-based Prometheus targets (node_exporter on every
# rig box, including the current spot fleet) and push them to the
# loadgen box. Run after any worker_instances change — Prometheus picks
# the files up without a restart.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

TARGETS=$(mktemp -d)
trap 'rm -rf "$TARGETS"' EXIT

{
    echo '['
    first=true
    for i in "${!CP_PRIVATE_IPS[@]}"; do
        $first || echo ','
        first=false
        printf '  {"targets":["%s:9100"],"labels":{"role":"control-plane","instance_name":"cp-%s"}}' \
            "${CP_PRIVATE_IPS[$i]}" "$i"
    done
    $first || echo ','
    printf '  {"targets":["%s:9100"],"labels":{"role":"loadgen","instance_name":"loadgen"}}\n' \
        "$LOADGEN_PRIVATE_IP"
    echo ']'
} >"$TARGETS/node-static.json"

{
    echo '['
    first=true
    while read -r instance_id _public private; do
        [ -n "$instance_id" ] || continue
        $first || echo ','
        first=false
        printf '  {"targets":["%s:9100"],"labels":{"role":"sim-worker","instance_name":"%s"}}' \
            "$private" "$instance_id"
    done < <(worker_instances)
    echo
    echo ']'
} >"$TARGETS/node-workers.json"

rcp "$TARGETS/node-static.json" "$LOADGEN_ADDR:/opt/bench/observability/targets/node-static.json"
rcp "$TARGETS/node-workers.json" "$LOADGEN_ADDR:/opt/bench/observability/targets/node-workers.json"
log "prometheus node targets updated ($(wc -l <"$TARGETS/node-workers.json") lines of workers)"
