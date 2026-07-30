#!/usr/bin/env bash
# One campaign cell: (system, node step, scenario) -> a driver run on
# the loadgen box, results pulled back into results/<campaign>/.
# Assumes the control plane and the sim fleet are already up at the
# right size (run-campaign.sh orchestrates that; this script verifies).
#
# Usage:
#   run-step.sh --system legion-a --nodes 1000 --scenario web_fleet \
#       [--multiplier 1.0] [--warmup 30] [--campaign <id>]
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

SYSTEM="" NODES="" SCENARIO="" MULTIPLIER="1.0" WARMUP="30" CAMPAIGN=""
while [ $# -gt 0 ]; do
    case "$1" in
    --system) SYSTEM="$2" ;;
    --nodes) NODES="$2" ;;
    --scenario) SCENARIO="$2" ;;
    --multiplier) MULTIPLIER="$2" ;;
    --warmup) WARMUP="$2" ;;
    --campaign) CAMPAIGN="$2" ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
    shift 2
done
: "${SYSTEM:?--system required}" "${NODES:?--nodes required}" "${SCENARIO:?--scenario required}"
CAMPAIGN="${CAMPAIGN:-adhoc-$(date -u +%Y%m%d-%H%M%S)}"

case "$SYSTEM" in
legion-a) DRIVER_SYSTEM=legion MANAGER="http://${CP_PRIVATE_IPS[0]}:4747" ;;
legion-b)
    DRIVER_SYSTEM=legion
    # Streaming: the primary is whichever box holds the lease.
    MANAGER=""
    for ip in "${CP_PRIVATE_IPS[@]}"; do
        if rsh "$LOADGEN_PUBLIC_IP" "curl -fsS -m 3 http://$ip:4749/readyz >/dev/null 2>&1"; then
            MANAGER="http://$ip:4747"
            break
        fi
    done
    [ -n "$MANAGER" ] || {
        echo "no streaming primary found" >&2
        exit 1
    }
    ;;
nomad) DRIVER_SYSTEM=nomad MANAGER="http://${CP_PRIVATE_IPS[0]}:4646" ;;
k8s) DRIVER_SYSTEM=k8s MANAGER="" ;;
*)
    echo "unknown system $SYSTEM (legion-a|legion-b|nomad|k8s)" >&2
    exit 1
    ;;
esac

RUN_ID="$SYSTEM-$SCENARIO-${NODES}n-x$MULTIPLIER"
REMOTE_OUT="/opt/bench/runs/$CAMPAIGN/$RUN_ID"
log "cell $RUN_ID (campaign $CAMPAIGN, manager ${MANAGER:-kubeconfig})"

# Legion cells fail fast if the fleet isn't at size — a cell run against
# a half-registered fleet is a wasted cluster-hour, the thing the
# dry-run gate exists to prevent.
if [ "$DRIVER_SYSTEM" = legion ]; then
    registered=$(rsh "$LOADGEN_PUBLIC_IP" \
        "LEGION_ADDR=$MANAGER legion state query 'SELECT COUNT(*) AS n FROM node' -j" |
        jq -r '.data.rows[0].n // 0')
    [ "$registered" -ge "$NODES" ] || {
        echo "fleet has $registered nodes, step needs $NODES — start-sim-fleet.sh first" >&2
        exit 1
    }
fi

rsh "$LOADGEN_PUBLIC_IP" "
    set -euo pipefail
    mkdir -p '$REMOTE_OUT'
    python3 /opt/bench/workloads/translate.py \
        --scenario '/opt/bench/scenarios/$SCENARIO.json' \
        --system '$DRIVER_SYSTEM' \
        --count-multiplier '$MULTIPLIER' \
        --out '$REMOTE_OUT/workload'
    python3 /opt/bench/driver/driver.py run \
        --system '$DRIVER_SYSTEM' \
        --workload-dir '$REMOTE_OUT/workload' \
        --out '$REMOTE_OUT/run' \
        ${MANAGER:+--manager '$MANAGER'} \
        --warmup-secs '$WARMUP' \
        --legion-bin /usr/local/bin/legion \
        | tee '$REMOTE_OUT/metrics.json'
"

"$(dirname "$0")/collect.sh" --campaign "$CAMPAIGN" --run "$RUN_ID" --system "$SYSTEM" \
    --nodes "$NODES" --scenario "$SCENARIO"
