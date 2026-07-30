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
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

SYSTEM="" STEPS="1000 2000 5000 10000" SCENARIOS="web_fleet batch_burst mixed_heterogeneous"
WARMUP=30 SMOKE=false STORM_STEP=5000 NODES_PER_INSTANCE=1250
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
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
done
: "${SYSTEM:?--system required (legion-a|legion-b|nomad|k8s)}"

CAMPAIGN="$(date -u +%Y%m%d-%H%M)-$SYSTEM"
SIM_SYSTEM=${SYSTEM%-a}
SIM_SYSTEM=${SIM_SYSTEM%-b} # legion-a/legion-b share the legion sim fleet
log "=== campaign $CAMPAIGN: steps [$STEPS], scenarios [$SCENARIOS] ==="

for NODES in $STEPS; do
    INSTANCES=$(((NODES + NODES_PER_INSTANCE - 1) / NODES_PER_INSTANCE))
    MULTIPLIER=$(python3 -c "print(max(0.25, $NODES/1000))")

    log "--- step ${NODES}n: scaling spot fleet to $INSTANCES instance(s) ---"
    terraform -chdir="$TF_DIR" apply -auto-approve -input=false \
        -var "worker_instances=$INSTANCES" >/dev/null
    # Spot capacity can take a couple of minutes to arrive.
    deadline=$(($(date +%s) + 600))
    while [ "$(worker_instances | wc -l)" -lt "$INSTANCES" ]; do
        (($(date +%s) < deadline)) || {
            echo "spot fleet did not reach $INSTANCES instances" >&2
            exit 1
        }
        sleep 15
    done
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
