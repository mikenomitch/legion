#!/usr/bin/env bash
# Storm scenario: kill ~10% of the simulated fleet at once (one instance
# slice), measure time-to-100%-replaced. Run with a steady workload
# already converged (run-campaign.sh invokes it right after the 5k-step
# cells, while that workload is still resident).
#
# Usage: storm.sh --system legion-a --nodes 5000 [--campaign <id>]
set -euo pipefail
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../scripts" && pwd)"
source "$SCRIPTS/env.sh"
load_rig

SYSTEM="" NODES="" CAMPAIGN=""
while [ $# -gt 0 ]; do
    case "$1" in
    --system) SYSTEM="$2" ;;
    --nodes) NODES="$2" ;;
    --campaign) CAMPAIGN="$2" ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
    shift 2
done
: "${SYSTEM:?}" "${NODES:?}"
CAMPAIGN="${CAMPAIGN:-adhoc-$(date -u +%Y%m%d-%H%M%S)}"

case "$SYSTEM" in
legion-*) DRIVER_SYSTEM=legion MANAGER="http://${CP_PRIVATE_IPS[0]}:4747" ;;
nomad) DRIVER_SYSTEM=nomad MANAGER="http://${CP_PRIVATE_IPS[0]}:4646" ;;
k8s) DRIVER_SYSTEM=k8s MANAGER="" ;;
esac

# The kill target: the first worker instance — with even slicing this is
# ~1/instances of the fleet (run at 8+ instances for the plan's "10% at
# 5k" shape; the fairness record captures the exact fraction).
read -r KILL_INSTANCE KILL_PUBLIC KILL_PRIVATE < <(worker_instances | head -1)
[ -n "$KILL_INSTANCE" ] || {
    echo "no worker instances" >&2
    exit 1
}

# How many allocations live on the doomed slice right now — that is the
# replacement expectation the watch waits for.
if [ "$DRIVER_SYSTEM" = legion ]; then
    EXPECTED=$(rsh "$LOADGEN_PUBLIC_IP" "LEGION_ADDR=$MANAGER legion state query \
        \"SELECT COUNT(*) AS n FROM allocation WHERE node_id IN (SELECT id FROM node WHERE id LIKE 'sim-0-%')\" -j" |
        jq -r '.data.rows[0].n // 0')
else
    EXPECTED=0 # nomad/k8s: wait out the timeout, read replaced_total
fi
log "storm: killing $KILL_INSTANCE (slice sim-0), expecting $EXPECTED replacements"

RUN_ID="experiments/storm-$(date -u +%Y%m%d-%H%M%S)"
REMOTE_OUT="/opt/bench/runs/$CAMPAIGN/$RUN_ID"
FAULT_CMD="ssh -o StrictHostKeyChecking=accept-new ubuntu@$KILL_PRIVATE sudo systemctl stop legion-sim nomad-nodesim 2>/dev/null || true"

rsh "$LOADGEN_PUBLIC_IP" "
    set -euo pipefail
    mkdir -p '$REMOTE_OUT'
    python3 /opt/bench/driver/driver.py watch \
        --system '$DRIVER_SYSTEM' \
        ${MANAGER:+--manager '$MANAGER'} \
        --out '$REMOTE_OUT/run' \
        --exec-after-secs 15 \
        --exec-cmd '$FAULT_CMD' \
        --expected-replacements '$EXPECTED' \
        --settle-timeout-secs 900 \
        --legion-bin /usr/local/bin/legion \
        | tee '$REMOTE_OUT/metrics.json'
"

"$SCRIPTS/collect.sh" --campaign "$CAMPAIGN" --run "$RUN_ID" --system "$SYSTEM" \
    --nodes "$NODES" --scenario storm

# The killed slice restarts (systemd) on the next fleet start; bring it
# back now so later cells see a full fleet.
rsh "$KILL_PUBLIC" "sudo systemctl start legion-sim 2>/dev/null || sudo systemctl start nomad-nodesim 2>/dev/null || true"
log "storm complete: see $RESULTS_ROOT/$CAMPAIGN/$RUN_ID/metrics.json (recovery_secs)"
