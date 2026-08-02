#!/usr/bin/env bash
# THE campaign runner: one command drives one system through every scale
# step and every workload, scaling the spot fleet between steps,
# collecting and archiving each cell. Re-runnable: each cell lands in
# its own directory keyed by the campaign id.
#
#   run-campaign.sh --system legion-a
#   run-campaign.sh --system legion-b --steps "1000 2000" --smoke
#   run-campaign.sh --system legion-a --steps "5000" --scenarios "batch_burst"
#
# Defaults follow the plan: steps 1k/2k/5k/10k; workloads
# steady-service (web_fleet), batch-burst, mixed+churn
# (mixed_heterogeneous); the storm drill runs via
# experiments/storm.sh at the 5k step. Workload allocation counts scale
# with the step (multiplier = nodes/1000) so pressure per node stays
# comparable across steps.
#
# Prerequisites (once per rig): bringup.sh, build-and-upload.sh, and the
# system's install script (install-legion-a.sh / -b / nomad / k8s).
#
# On exit — however it exits — the spot fleet is released, so a campaign
# that dies unattended stops costing money. Pass --keep-fleet to hold the
# fleet up for a post-mortem. For overnight runs, launch this on the
# loadgen box via scripts/remote-campaign.sh rather than from a laptop.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

SYSTEM="" STEPS="1000 2000 5000 10000" SCENARIOS="web_fleet batch_burst mixed_heterogeneous"
# Storm granularity is one sim process (= one instance slice), so the
# drill runs at the 10k step where a slice is 12.5% of the fleet — the
# closest process-granular approximation of the plan's "kill 10%"; the
# exact fraction lands in the fairness record.
WARMUP=30 SMOKE=false STORM_STEP=10000 NODES_PER_INSTANCE=1250 KEEP_FLEET=false
while [ $# -gt 0 ]; do
    case "$1" in
    --system)
        SYSTEM="$2"
        shift 2
        ;;
    --steps)
        STEPS="$2"
        shift 2
        ;;
    --scenarios)
        SCENARIOS="$2"
        shift 2
        ;;
    --warmup)
        WARMUP="$2"
        shift 2
        ;;
    --smoke)
        # Phase-1 exit criterion: one small step, one workload, then out.
        STEPS="1000" SCENARIOS="web_fleet" SMOKE=true
        shift
        ;;
    --keep-fleet)
        KEEP_FLEET=true
        shift
        ;;
    --campaign)
        CAMPAIGN_OVERRIDE="$2"
        shift 2
        ;;
    --nodes-per-instance)
        # The measured ceiling from experiments/packing-density.sh. The
        # 1250 default is the plan's estimate; measuring it once turns
        # every step's instance count (and its spot vCPU bill) into a
        # number backed by this fleet rather than by an assumption.
        NODES_PER_INSTANCE="$2"
        shift 2
        ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
done
: "${SYSTEM:?--system required (legion-a|legion-b|nomad|k8s)}"

# Release the spot fleet no matter how we leave — success, failure, or a
# killed session. An unattended campaign that dies at 03:00 otherwise
# leaves up to 20 spot instances running until someone notices, and the
# fleet is the expensive half of the rig. The control plane deliberately
# stays up: it holds the state a post-mortem needs, and teardown.sh is
# the one thing that should ever destroy it.
release_fleet() {
    local code=$?
    if $KEEP_FLEET; then
        log "leaving the spot fleet up (--keep-fleet)"
        return $code
    fi
    log "releasing the spot fleet (exit $code)"
    "$(dirname "$0")/stop-sim-fleet.sh" >/dev/null 2>&1 || true
    scale_workers 0 >/dev/null 2>&1 || log "WARNING: could not scale the fleet to 0 — check the ASG"
    return $code
}
trap release_fleet EXIT
# Bash does not run an EXIT trap when it dies on an untrapped signal, and
# `remote-campaign.sh stop` kills the tmux session with a SIGHUP. Turn
# those into a normal exit so the release above still happens.
trap 'exit 143' HUP INT TERM

# An explicit id lets the caller correlate archives with a status stream
# it already published under (remote-campaign.sh does exactly this).
CAMPAIGN="${CAMPAIGN_OVERRIDE:-$(date -u +%Y%m%d-%H%M)-$SYSTEM}"
SIM_SYSTEM=${SYSTEM%-a}
SIM_SYSTEM=${SIM_SYSTEM%-b} # legion-a/legion-b share the legion sim fleet
log "=== campaign $CAMPAIGN: steps [$STEPS], scenarios [$SCENARIOS] ==="

for NODES in $STEPS; do
    INSTANCES=$(((NODES + NODES_PER_INSTANCE - 1) / NODES_PER_INSTANCE))
    MULTIPLIER=$(python3 -c "print(max(0.25, $NODES/1000))")

    log "--- step ${NODES}n: scaling spot fleet to $INSTANCES instance(s) ---"
    scale_workers "$INSTANCES"
    # Spot capacity can take a couple of minutes to arrive.
    wait_for_workers "$INSTANCES" 600
    "$(dirname "$0")/gen-targets.sh"

    "$(dirname "$0")/stop-sim-fleet.sh"
    "$(dirname "$0")/start-sim-fleet.sh" "$SIM_SYSTEM" "$NODES"

    for SCENARIO in $SCENARIOS; do
        log "--- cell: $SCENARIO @ ${NODES}n (x$MULTIPLIER) ---"
        "$(dirname "$0")/run-step.sh" \
            --system "$SYSTEM" --nodes "$NODES" --scenario "$SCENARIO" \
            --multiplier "$MULTIPLIER" --warmup "$WARMUP" --campaign "$CAMPAIGN"
    done

    if [ "$NODES" -eq "$STORM_STEP" ] && ! $SMOKE; then
        log "--- storm drill @ ${NODES}n ---"
        "$AWS_BENCH_ROOT/experiments/storm.sh" --system "$SYSTEM" --nodes "$NODES" \
            --campaign "$CAMPAIGN" || log "WARNING: storm drill failed (continuing)"
    fi
done

"$(dirname "$0")/stop-sim-fleet.sh"

log "=== campaign $CAMPAIGN complete ==="
log "results: $RESULTS_ROOT/$CAMPAIGN (and s3://$BUCKET/campaigns/$CAMPAIGN/)"
log "analysis: python3 $AWS_BENCH_ROOT/analysis/fit_curves.py --campaign $RESULTS_ROOT/$CAMPAIGN"
if $SMOKE; then
    log "smoke passed — phase 1 exit: one command up, one command down, metrics landing"
fi
