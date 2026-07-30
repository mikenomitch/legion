#!/usr/bin/env bash
# Stop every simulated fleet (all systems) on the current worker
# instances, and delete KWOK nodes if a kubeconfig is present. Used
# between scale steps and between campaigns.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

while read -r instance_id public _private; do
    [ -n "$instance_id" ] || continue
    log "stopping sim units on $instance_id"
    rsh "$public" "
        sudo systemctl disable --now legion-sim 2>/dev/null || true
        sudo systemctl disable --now nomad-nodesim 2>/dev/null || true
    " || true
done < <(worker_instances)

rsh "$LOADGEN_PUBLIC_IP" \
    "command -v kubectl >/dev/null 2>&1 && kubectl delete nodes -l type=kwok --ignore-not-found" || true
log "sim fleets stopped"
