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
        sudo systemctl stop 'nomad-nodesim@*' 2>/dev/null || true
        sudo systemctl disable --now kwok-slice 2>/dev/null || true
    " || true
done < <(worker_instances)

# One server-side deletecollection. `kubectl delete nodes -l type=kwok`
# deletes one object per request and ran for over 30 minutes on a
# 10,000-node fleet, stalling the campaign. Bounded by `timeout` because a
# control plane that is already gone must not hang a teardown.
rsh "$LOADGEN_ADDR" \
    "command -v kubectl >/dev/null 2>&1 && timeout 300 kubectl delete --raw '/api/v1/nodes?labelSelector=type%3Dkwok' >/dev/null" || true

# Retract the sim-worker scrape targets published by start-sim-fleet.sh, so
# a stopped fleet reads as "no targets" rather than a wall of `up == 0`.
rsh "$LOADGEN_ADDR" \
    "echo '[]' >/opt/bench/observability/targets/sim-workers.json" || true

# Forget the recorded fleet. Nothing here removes the NODE RECORDS — there
# is no deregistration RPC, so every node this fleet registered stays in
# the store, `lost`, until a fleet of the same names re-registers over it
# or the control plane is reset (scripts/reset-control-plane.sh). What
# this clears is only the claim that a fleet is running, so the next
# campaign does not reuse a fleet that has just been shut down.
clear_fleet_state

log "sim fleets stopped (node records remain — see reset-control-plane.sh)"
