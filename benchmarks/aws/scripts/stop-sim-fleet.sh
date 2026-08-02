#!/usr/bin/env bash
# Stop every simulated fleet (all systems) on the current worker
# instances, and delete KWOK nodes if a kubeconfig is present. Used
# between scale steps and between campaigns.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

while read -r instance_id public private; do
    [ -n "$instance_id" ] || continue
    log "stopping sim units on $instance_id"
    rsh "$(worker_addr "$public" "$private")" "
        sudo systemctl disable --now legion-sim 2>/dev/null || true
        sudo systemctl disable --now nomad-nodesim 2>/dev/null || true
    " || true
done < <(worker_instances)

rsh "$LOADGEN_ADDR" \
    "command -v kubectl >/dev/null 2>&1 && kubectl delete nodes -l type=kwok --ignore-not-found" || true

# Retract the sim-worker scrape targets published by start-sim-fleet.sh, so
# a stopped fleet reads as "no targets" rather than a wall of `up == 0`.
rsh "$LOADGEN_ADDR" \
    "echo '[]' >/opt/bench/observability/targets/sim-workers.json" || true

log "sim fleets stopped"
