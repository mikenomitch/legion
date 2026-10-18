#!/usr/bin/env bash
# One campaign cell: (system, node step, scenario) -> a driver run on
# the loadgen box, results pulled back into results/<campaign>/.
# Assumes the control plane and the sim fleet are already up at the
# right size (run-campaign.sh orchestrates that; this script verifies).
#
# Usage:
#   run-step.sh --system legion-a --nodes 1000 --scenario web_fleet \
#       [--target-allocations 5000 | --multiplier 1.0] \
#       [--warmup 30] [--campaign <id>]
#
# The cell's run id names its sizing (`-a<allocations>` or
# `-x<multiplier>`), so several allocation densities at one node count
# are several archives rather than one overwritten one.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

SYSTEM="" NODES="" SCENARIO="" MULTIPLIER="1.0" WARMUP="30" CAMPAIGN=""
# Leave the cell's jobs resident on exit. The storm drill measures
# time-to-replace for allocations lost with their nodes, so it needs a
# workload RESIDENT when the fault lands — the 2026-08-06 drill ran after
# the cell's cleanup and measured a 1,250-node loss with nothing on it:
# loss-path metrics only, replacement throughput unmeasured.
KEEP_JOBS=false
# Jobs submitted in parallel. Left empty so the driver picks the default that
# suits the submission path it takes (a CLI slot is a whole process; a native
# slot is one in-flight RPC, and the two want very different numbers). Set
# LEGION_SUBMIT_CONCURRENCY or --submit-concurrency to pin it.
SUBMIT_CONCURRENCY="${LEGION_SUBMIT_CONCURRENCY:-}"
SUBMIT_RATE=""
# How Legion jobs are submitted.
#
# `auto` takes the native submitter (legion-loadgen) when the loadgen box has
# it. That path exists because the per-job CLI loop ceilinged at ~36-103
# jobs/sec and phase 4 reported that ceiling as Legion's throughput while the
# control plane idled at 8% CPU.
#
# FAIRNESS: it is Legion-only. Nomad and K8s still spawn a process per job, so
# any cell meant for a cross-system comparison must run with
# LEGION_SUBMITTER=cli (all three on the shared loop). The driver archives the
# path it took as `submitter` in metrics.json, so this can be verified after
# the fact rather than remembered.
SUBMITTER="${LEGION_SUBMITTER:-auto}"
# Preferred over --multiplier: sizes the cell to an allocation count
# rather than to a scale factor over each scenario's own base, which
# differ (180/304/380) and so scatter under one shared multiplier.
TARGET_ALLOCS=""
while [ $# -gt 0 ]; do
    case "$1" in
    --system) SYSTEM="$2" ;;
    --nodes) NODES="$2" ;;
    --scenario) SCENARIO="$2" ;;
    --multiplier) MULTIPLIER="$2" ;;
    --target-allocations) TARGET_ALLOCS="$2" ;;
    --warmup) WARMUP="$2" ;;
    # Value-taking (`--keep-jobs true`) because this loop shifts two for
    # every argument.
    --keep-jobs) KEEP_JOBS="$2" ;;
    --submit-concurrency) SUBMIT_CONCURRENCY="$2" ;;
    # Open-loop jobs/sec for the native Legion submitter (0 = burst).
    --submit-rate) SUBMIT_RATE="$2" ;;
    --submitter) SUBMITTER="$2" ;;
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
        if rsh "$LOADGEN_ADDR" "curl -fsS -m 3 http://$ip:4749/readyz >/dev/null 2>&1"; then
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

# The run id has to name the SIZE of the cell, not just its node count.
# With --target-allocations the multiplier stays at its default 1.0, so
# every point of an allocation sweep at one node count produced the id
# `<system>-<scenario>-1000n-x1.0` and overwrote the one before it —
# three cells in, two of them gone, and nothing in the archive to say so.
# `-a<allocations>` is the sizing that was actually requested;
# analysis/fit_curves.py parses both forms.
if [ -n "$TARGET_ALLOCS" ]; then
    RUN_ID="$SYSTEM-$SCENARIO-${NODES}n-a$TARGET_ALLOCS"
else
    RUN_ID="$SYSTEM-$SCENARIO-${NODES}n-x$MULTIPLIER"
fi
# A paced cell measures latency, not throughput, so it must never share an
# id with (and overwrite) the burst cell at the same size.
if [ -n "$SUBMIT_RATE" ]; then
    RUN_ID="$RUN_ID-r$SUBMIT_RATE"
fi
REMOTE_OUT="/opt/bench/runs/$CAMPAIGN/$RUN_ID"
log "cell $RUN_ID (campaign $CAMPAIGN, manager ${MANAGER:-kubeconfig})"

# Legion cells fail fast unless the fleet is EXACTLY this cell's fleet —
# a cell run against the wrong fleet is a wasted cluster-hour, the thing
# this gate exists to prevent.
#
# It used to gate on `registered >= NODES` from
# `SELECT COUNT(*) FROM node`, which is a lower bound on the wrong
# population. Nothing in Legion removes a node record (there is no
# deregistration RPC), so that count is the high-water mark of every
# fleet the control plane has ever seen: a 4000-node experiment left 3000
# orphans, and the next 1000-node cell sailed through the gate with
# total=4000 while reporting itself as a 1000-node measurement. The count
# also ignores health, so a fleet in which every node had gone `lost`
# passed too. assert_fleet (scripts/env.sh) asserts the exact count, all
# active, all named from this fleet's slices, and prints the census it
# actually saw when it refuses.
PRE_CELL_EVAL_DEPTH=-1
if [ "$DRIVER_SYSTEM" = legion ]; then
    # Slice count from what start-sim-fleet.sh recorded; falling back to
    # the current instance count for a hand-driven run-step.sh, since
    # start-sim-fleet.sh gives slice i to the i-th InService instance.
    SLICES=$(fleet_state | jq -r '.slices // empty')
    if [ -z "$SLICES" ]; then
        SLICES=$(worker_instances | grep -c . || true)
        log "no recorded fleet state; assuming $SLICES slice(s) from the ASG"
    fi
    assert_fleet "$MANAGER" "$NODES" "$SLICES" 300 || exit 1

    # Then wait out the churn before measuring anything. Every fleet
    # stop/start marks thousands of nodes lost, and each loss raises a
    # reschedule eval that competes with this cell's submissions for the
    # same eval workers — a 10k-node restart measured 5760 reschedules
    # against 2503 submits, so the cell's own work was a minority of what
    # the scheduler was doing and its "throughput" was really a share of
    # a contended queue. Waiting is cheap; a mis-attributed cell is not.
    log "waiting for the pre-cell eval backlog to drain"
    PRE_CELL_EVAL_DEPTH=$(wait_for_eval_drain "$MANAGER" 10 600)
else
    # The competitors' version of the same gate: exactly this cell's
    # fleet, every node ready, or no cell.
    wait_for_ready_nodes "$DRIVER_SYSTEM" "$NODES" 300 || exit 1
fi

# Leave the cluster as this cell found it. Cells run back to back -- three
# scenarios per step, four steps -- and nothing else removes what a cell
# submitted, so without this the 10k step is measured against a cluster
# still carrying every job the 1k, 2k and 5k steps left behind. That
# would show up as throughput decaying across a campaign for reasons
# unrelated to node count, which is precisely the variable being fitted.
#
# Collect first, destroy second: the archive is written from the running
# cluster's metrics, so tearing down before collecting loses the cell.
#
# Enumerated by state query rather than `job list` because that verb
# paginates and would silently leave the tail behind. Destroys run in
# parallel for the same reason submission does -- 8k jobs at 25ms each is
# minutes of rig time otherwise.
cleanup_jobs() {
    case "$DRIVER_SYSTEM" in
    nomad)
        cleanup_nomad_jobs
        return
        ;;
    k8s)
        cleanup_k8s_jobs
        return
        ;;
    esac
    log "destroying this cell's jobs"
    destroyed=$(rsh "$LOADGEN_ADDR" "
        export LEGION_ADDR=$MANAGER
        names=\$(legion state query 'SELECT name FROM job' -j 2>/dev/null |
            jq -r '.data.rows[].name // empty')
        [ -z \"\$names\" ] && { echo 0; exit 0; }
        # Stop, then destroy. A running job cannot be destroyed (the
        # destroy fails with FailedPrecondition, 'must be stopped'), and
        # the output below goes to /dev/null, so skipping the stop would
        # leave every job behind without a word.
        printf '%s\n' \"\$names\" |
            xargs -r -P 8 -n 1 -I{} legion job stop {} >/dev/null 2>&1 || true
        sleep 5
        printf '%s\n' \"\$names\" |
            xargs -r -P 8 -n 1 -I{} legion job destroy {} >/dev/null 2>&1 || true
        legion state query 'SELECT COUNT(*) AS n FROM job' -j 2>/dev/null |
            jq -r '.data.rows[0].n'
    ")
    # A non-zero remainder is not fatal -- the next cell's numbers are
    # what it endangers, so say it loudly and let the campaign decide.
    [ "${destroyed:-0}" = 0 ] ||
        log "WARNING: $destroyed jobs survived teardown — the next cell starts dirty"
}

# Nomad: purge every bench job (stop + remove in one call), then GC, then
# wait for the allocations to be gone so the next cell starts empty.
cleanup_nomad_jobs() {
    log "purging this cell's nomad jobs"
    remaining=$(rsh "$LOADGEN_ADDR" "
        export NOMAD_ADDR=$MANAGER
        curl -fsS \$NOMAD_ADDR/v1/jobs?prefix=bench- | jq -r '.[].ID' |
            xargs -r -P 16 -I{} curl -fsS -o /dev/null -X DELETE '$MANAGER/v1/job/{}?purge=true'
        curl -fsS -o /dev/null -X PUT \$NOMAD_ADDR/v1/system/gc || true
        for _ in \$(seq 1 60); do
            n=\$(curl -fsS \$NOMAD_ADDR/v1/allocations | jq '[.[] | select((.JobID | startswith(\"bench-\")) and .DesiredStatus == \"run\")] | length')
            [ \"\$n\" = 0 ] && break
            sleep 5
        done
        curl -fsS \$NOMAD_ADDR/v1/jobs?prefix=bench- | jq length
    ")
    [ "${remaining:-0}" = 0 ] ||
        log "WARNING: $remaining nomad jobs survived teardown — the next cell starts dirty"
}

# K8s: server-side deletecollection rather than `kubectl delete` per
# object, which issues one sequential DELETE per Deployment — minutes at
# 10k objects. Pods are collected too instead of waiting on the garbage
# collector, and the cell is not over until none are left.
cleanup_k8s_jobs() {
    # One server-side delete of the bench namespace, then recreate it.
    # Deleting by label (deletecollection on deployments, replicasets, and
    # pods) left 15,626 pods behind at 5,000 nodes: the Deployments
    # survived and kept recreating them. A namespace delete takes every
    # object with it, and the cell is not over until the namespace is gone.
    log "deleting the bench namespace"
    remaining=$(rsh "$LOADGEN_ADDR" "
        kubectl delete namespace bench --wait=true --timeout=20m >/dev/null 2>&1 || true
        kubectl get namespace bench >/dev/null 2>&1 && echo stuck && exit 0
        kubectl create namespace bench >/dev/null && echo 0
    ")
    [ "${remaining:-}" = 0 ] ||
        log "WARNING: the bench namespace did not finish deleting — the next cell starts dirty"
}

# On EXIT, not inline. The driver exits non-zero whenever a cell is
# incomplete, and under `pipefail` that aborts this script -- so an
# inline teardown ran only after a SUCCESSFUL cell, which is precisely
# backwards: a failed cell is the one most likely to have left a
# half-placed workload behind to poison its successor. That is what
# happened here, and the symptom was baffling rather than obvious --
# the next cell resubmitted identical specs for jobs that already
# existed, Legion correctly treated it as a no-op, no allocations were
# created, no placement events fired, and the cell reported zero
# placements as though the event stream were broken.
trap '[ "$KEEP_JOBS" = true ] || cleanup_jobs' EXIT

# One or the other, never both: --target-allocations wins when given, and
# the chosen sizing is echoed so the cell's log says which it was.
if [ -n "$TARGET_ALLOCS" ]; then
    SIZE_ARG="--target-allocations '$TARGET_ALLOCS'"
    SIZING_DESC="~$TARGET_ALLOCS allocations"
else
    SIZE_ARG="--count-multiplier '$MULTIPLIER'"
    SIZING_DESC="multiplier x$MULTIPLIER"
fi
log "sizing: $SIZING_DESC"

# Stamped before the driver starts so collect.sh can scope its Prometheus
# snapshots to this cell rather than to a trailing window.
CELL_START_EPOCH=$(date -u +%s)
export CELL_START_EPOCH

# What the cluster looked like the instant before this cell measured
# anything, archived with the cell. Without it a contaminated cell is
# indistinguishable from a clean one after the fact: the eval backlog we
# started from is the difference between "the scheduler placed 900/sec"
# and "the scheduler placed 900/sec while also chewing through 5760
# reschedules from the fleet restart we just did".
export PRE_CELL_EVAL_DEPTH
if [ "$DRIVER_SYSTEM" = legion ]; then
    PRE_CELL_CENSUS=$(node_census "$MANAGER" "${SLICES:-0}")
else
    PRE_CELL_CENSUS='{}'
fi
rsh "$LOADGEN_ADDR" "mkdir -p '$REMOTE_OUT' && cat >'$REMOTE_OUT/pre-cell.json'" <<<"$(
    jq -n --argjson census "$PRE_CELL_CENSUS" \
        --argjson eval_backlog "${PRE_CELL_EVAL_DEPTH:--1}" \
        --argjson nodes "$NODES" --arg scenario "$SCENARIO" \
        --arg sizing "$SIZING_DESC" \
        --argjson start_epoch "$CELL_START_EPOCH" \
        '{nodes: $nodes, scenario: $scenario, sizing: $sizing,
          cell_start_epoch: $start_epoch,
          node_census: $census,
          eval_backlog_before_cell: $eval_backlog,
          note: "eval_backlog_before_cell > 0 means this cell shared the eval workers with fleet churn"}'
)"

# K8s cells run in their own namespace (see cleanup_k8s_jobs).
if [ "$DRIVER_SYSTEM" = k8s ]; then
    rsh "$LOADGEN_ADDR" "kubectl get namespace bench >/dev/null 2>&1 || kubectl create namespace bench >/dev/null"
fi

rsh "$LOADGEN_ADDR" "
    set -euo pipefail
    mkdir -p '$REMOTE_OUT'
    python3 /opt/bench/workloads/translate.py \
        --scenario '/opt/bench/scenarios/$SCENARIO.json' \
        --system '$DRIVER_SYSTEM' \
        $SIZE_ARG \
        --out '$REMOTE_OUT/workload'
    python3 /opt/bench/driver/driver.py run \
        --system '$DRIVER_SYSTEM' \
        --workload-dir '$REMOTE_OUT/workload' \
        --out '$REMOTE_OUT/run' \
        ${MANAGER:+--manager '$MANAGER'} \
        --warmup-secs '$WARMUP' \
        ${SUBMIT_CONCURRENCY:+--submit-concurrency '$SUBMIT_CONCURRENCY'} \
        ${SUBMIT_RATE:+--submit-rate '$SUBMIT_RATE'} \
        --submitter '$SUBMITTER' \
        --legion-bin /usr/local/bin/legion \
        --legion-loadgen-bin /usr/local/bin/legion-loadgen \
        | tee '$REMOTE_OUT/metrics.json'
"

"$(dirname "$0")/collect.sh" --campaign "$CAMPAIGN" --run "$RUN_ID" --system "$SYSTEM" \
    --nodes "$NODES" --scenario "$SCENARIO"

