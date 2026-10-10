#!/usr/bin/env bash
# Storm scenario: kill ~10% of the simulated fleet at once (one instance
# slice), measure time-to-100%-replaced. Run with a steady workload
# already converged (run-campaign.sh invokes it right after the 5k-step
# cells, while that workload is still resident).
#
# Usage: storm.sh --system legion-a --nodes 5000 [--campaign <id>] [--settle-timeout <s>]
set -euo pipefail
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../scripts" && pwd)"
source "$SCRIPTS/env.sh"
load_rig

SYSTEM="" NODES="" CAMPAIGN="" SETTLE=""
while [ $# -gt 0 ]; do
    case "$1" in
    --system) SYSTEM="$2" ;;
    --nodes) NODES="$2" ;;
    --campaign) CAMPAIGN="$2" ;;
    --settle-timeout) SETTLE="$2" ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
    shift 2
done
: "${SYSTEM:?}" "${NODES:?}"
CAMPAIGN="${CAMPAIGN:-adhoc-$(date -u +%Y%m%d-%H%M%S)}"

# Each system's slice-0 node-name prefix (start-sim-fleet.sh), and how
# long to wait for replacement. K8s gets longer on purpose: at stock
# settings it does not evict from a NotReady node for 300s
# (DefaultTolerationSeconds) and then taints at most 0.1 nodes/sec
# (--node-eviction-rate), so a 1,250-node slice takes hours to drain.
# The drill measures that honestly rather than tuning it away, and caps
# the watch so the cell reports a replaced fraction and a rate instead of
# blocking a campaign until morning.
case "$SYSTEM" in
legion-*) DRIVER_SYSTEM=legion MANAGER="http://${CP_PRIVATE_IPS[0]}:4747" SLICE0=sim-0- DEFAULT_SETTLE=900 ;;
nomad) DRIVER_SYSTEM=nomad MANAGER="http://${CP_PRIVATE_IPS[0]}:4646" SLICE0=nsim-0- DEFAULT_SETTLE=900 ;;
k8s) DRIVER_SYSTEM=k8s MANAGER="" SLICE0=kwok-0- DEFAULT_SETTLE=1800 ;;
*)
    echo "unknown system $SYSTEM" >&2
    exit 1
    ;;
esac
SETTLE="${SETTLE:-$DEFAULT_SETTLE}"

# The kill target: the first worker instance — with even slicing this is
# ~1/instances of the fleet (run at 8+ instances for the plan's "10% at
# 5k" shape; the fairness record captures the exact fraction).
read -r KILL_INSTANCE KILL_PUBLIC KILL_PRIVATE < <(worker_instances | head -1)
[ -n "$KILL_INSTANCE" ] || {
    echo "no worker instances" >&2
    exit 1
}

# How many allocations live on the doomed slice right now — that is the
# replacement expectation the watch waits for. Every system counts its
# own way, but the population is the same: live bench work on slice 0.
case "$DRIVER_SYSTEM" in
legion)
    EXPECTED=$(rsh "$LOADGEN_ADDR" "LEGION_ADDR=$MANAGER legion state query \
        \"SELECT COUNT(*) AS n FROM allocation WHERE node_id IN (SELECT id FROM node WHERE id LIKE '$SLICE0%')\" -j" |
        jq -r '.data.rows[0].n // 0')
    ;;
nomad)
    EXPECTED=$(rsh "$LOADGEN_ADDR" "curl -fsS $MANAGER/v1/allocations" |
        jq --arg p "$SLICE0" '[.[] | select((.NodeName // "" | startswith($p))
            and .DesiredStatus == "run" and (.JobID | startswith("bench-")))] | length')
    ;;
k8s)
    EXPECTED=$(rsh "$LOADGEN_ADDR" "kubectl get pods -n bench -l bench=1 -o json" |
        jq --arg p "$SLICE0" '[.items[] | select(.spec.nodeName // "" | startswith($p))] | length')
    ;;
esac
log "storm: killing $KILL_INSTANCE (slice ${SLICE0%-}), expecting $EXPECTED replacements"

RUN_ID="experiments/storm-$(date -u +%Y%m%d-%H%M%S)"
REMOTE_OUT="/opt/bench/runs/$CAMPAIGN/$RUN_ID"
# The simulator that owns the slice. No quotes inside FAULT_CMD: it is
# embedded in single quotes below and run by the driver through a shell.
# nodesim runs one unit per profile, hence the template glob.
case "$DRIVER_SYSTEM" in
legion) SIM_UNIT=legion-sim ;;
nomad) SIM_UNIT=nomad-nodesim@\\* ;;
k8s) SIM_UNIT=kwok-slice ;;
esac
FAULT_CMD="$RSH_FROM_LOADGEN ubuntu@$KILL_PRIVATE sudo systemctl stop $SIM_UNIT"

rsh "$LOADGEN_ADDR" "
    set -euo pipefail
    mkdir -p '$REMOTE_OUT'
    python3 /opt/bench/driver/driver.py watch \
        --system '$DRIVER_SYSTEM' \
        ${MANAGER:+--manager '$MANAGER'} \
        --out '$REMOTE_OUT/run' \
        --exec-after-secs 15 \
        --exec-cmd '$FAULT_CMD' \
        --expected-replacements '$EXPECTED' \
        --settle-timeout-secs '$SETTLE' \
        --legion-bin /usr/local/bin/legion \
        | tee '$REMOTE_OUT/metrics.json'
"

"$SCRIPTS/collect.sh" --campaign "$CAMPAIGN" --run "$RUN_ID" --system "$SYSTEM" \
    --nodes "$NODES" --scenario storm

# The killed slice restarts (systemd) on the next fleet start; bring it
# back now so later cells see a full fleet — and they now REQUIRE it,
# because the fleet gate asserts an exact census rather than a lower
# bound, so a slice left stopped fails the next cell outright.
#
# Addressed with worker_addr, not the public IP: on-rig (the unattended
# path, which is how a campaign actually runs) traffic to another
# instance's public IP hairpins out through the internet gateway and no
# longer matches the security group's intra-rig rule, so this hop was
# refused and the slice silently stayed down.
rsh "$(worker_addr "$KILL_PUBLIC" "$KILL_PRIVATE")" '
    if systemctl cat legion-sim >/dev/null 2>&1; then sudo systemctl start legion-sim; fi
    if systemctl cat kwok-slice >/dev/null 2>&1; then sudo systemctl start kwok-slice; fi
    for c in /etc/nodesim/*.hcl; do
        [ -e "$c" ] && sudo systemctl start "nomad-nodesim@$(basename "$c" .hcl)"
    done
    true
'
log "storm complete: see $RESULTS_ROOT/$CAMPAIGN/$RUN_ID/metrics.json (recovery_secs)"
