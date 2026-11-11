#!/usr/bin/env bash
# Stop every simulated fleet (all systems) on the current worker
# instances, forget the Legion nodes they registered, and delete KWOK
# nodes if a kubeconfig is present. Used between scale steps and between
# campaigns.
#
#   stop-sim-fleet.sh [manager-addr]
#
# manager-addr defaults to the one start-sim-fleet.sh recorded in the
# fleet state, else the first control-plane box. SIM_FORGET=0 skips the
# forget, for a caller that wipes the store next anyway
# (reset-control-plane.sh).
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

# Read before anything below clears it.
MANAGER="${1:-$(fleet_state | jq -r '.manager // empty')}"
MANAGER="${MANAGER:-http://${CP_PRIVATE_IPS[0]}:4747}"

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

# Forget the Legion sim nodes, now that nothing will heartbeat for them
# again. Without this every node the fleet registered stays in the store,
# `lost`, and the node table becomes a high-water mark of every fleet the
# control plane has carried. `forget_sim_nodes` (env.sh) forgets each
# `sim-*` row with `legion node forget --force`; on a Nomad or K8s
# control plane the Legion query fails and nothing is forgotten.
if [ "${SIM_FORGET:-1}" = 0 ]; then
    LEFT=skipped
else
    log "forgetting $SIM_NAME_PREFIX-* nodes on $MANAGER"
    LEFT=$(forget_sim_nodes "$MANAGER")
fi
case "$LEFT" in
skipped) log "not forgetting node records (SIM_FORGET=0)" ;;
-1) log "no Legion manager answered at $MANAGER; no node records forgotten" ;;
0) log "every $SIM_NAME_PREFIX-* node record forgotten" ;;
*) log "WARNING: $LEFT $SIM_NAME_PREFIX-* node record(s) remain after the forget; the next" \
    "fleet's census gate will refuse them (reset-control-plane.sh clears everything)" ;;
esac

# Clear the claim that a fleet is running, so the next campaign does not
# reuse a fleet that has just been shut down.
clear_fleet_state

log "sim fleets stopped"
