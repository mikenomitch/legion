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
# SATURATION SWEEP. To push one node count until the scheduler stops
# keeping up, sweep the allocation density instead of the node count —
# one cell per point, all on one fleet, each archived separately:
#
#   run-campaign.sh --system legion-a --steps "5000" \
#       --allocs-per-node-sweep "5 10 20 40 80"
#
# EVAL CONCURRENCY. --eval-threads N renders the manager's evalThreads
# for this campaign (legion-a only), so the phase-2 finding that more
# planners cost throughput can be re-measured in one command rather than
# by editing a committed template between runs:
#
#   run-campaign.sh --system legion-a --steps "1000 5000" --eval-threads 16
#
# CONTROL-PLANE RESET. Every campaign wipes and reinstalls the control
# plane before its first step (--no-reset-control-plane opts out). This
# is not tidiness: Legion cannot deregister a node, so the node table
# accumulates every fleet the boxes have ever seen, and a campaign that
# follows a wider run measures a scheduler carrying that run's corpses
# while reporting its own node count. See reset-control-plane.sh.
#
# ONE SCENARIO PER INVOCATION. The fleet is shaped from the scenario's
# own node_profiles, so a campaign covering several would have to reshape
# the fleet mid-run, orphaning the previous scenario's nodes (node names
# embed the profile name and nothing can deregister a node). Passing more
# than one is refused with the commands to run instead. The plan's three
# workloads are therefore three campaigns, with a control-plane wipe
# between them.
#
# Defaults follow the plan: steps 1k/2k/5k/10k; workloads
# steady-service (web_fleet), batch-burst, mixed+churn
# (mixed_heterogeneous); the storm drill runs via
# experiments/storm.sh at the 10k step, for the granularity reason on
# STORM_STEP below — the plan asks for 5k, and this is the deviation.
# Each cell is sized to --allocs-per-node * nodes ALLOCATIONS, so
# pressure per node stays comparable across steps and every step is large
# enough to measure (see the sizing comment on the cell loop).
#
# Prerequisites (once per rig): bringup.sh and build-and-upload.sh. The
# system's install script runs as part of the reset at campaign start, so
# it does not have to be run by hand first.
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
# Allocations per node per cell. 5 puts the four steps at 5k/10k/25k/50k,
# which is the plan's stated band.
ALLOCS_PER_NODE=5
# A saturation sweep: several allocation densities at the SAME node
# count, one cell each. Empty means "just --allocs-per-node".
ALLOCS_SWEEP=""
# Wipe + reinstall the control plane before the first step. On by
# default because it is the only way to empty the node table — see
# reset-control-plane.sh for why, and why unique node names are not the
# answer.
RESET_CP=true
# Scheduler eval concurrency for the system under test. Empty means "the
# installer's default" (4), so an ordinary campaign is unaffected. Set,
# it is exported for the reinstall that the reset below performs — which
# is the only reason this belongs here rather than being a one-off
# install-legion-a.sh invocation: every campaign reinstalls the control
# plane, so a value set only at install time would be reverted before
# the first cell ran.
EVAL_THREADS=""
# Empty leaves run-step.sh's default. Raise it when a cell reports
# submission.driver_bound, which means the driver set the measured rate.
SUBMIT_CONCURRENCY=""
# Threaded to run-step so a storm drill after the last cell finds a
# workload still resident (see run-step.sh).
KEEP_JOBS=false
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
    --allocs-per-node)
        # Raise to push a step harder without changing its node count;
        # the two axes are deliberately separate.
        ALLOCS_PER_NODE="$2"
        shift 2
        ;;
    --allocs-per-node-sweep)
        # The saturation curve, as one command:
        #   run-campaign.sh --system legion-a --steps 5000 \
        #       --allocs-per-node-sweep "5 10 20 40"
        # Four cells at 5000 nodes and 25k/50k/100k/200k allocations,
        # each archived under its own run id (<...>-5000n-a<target>), on
        # ONE fleet — the points share a fleet precisely because a
        # restart between them would put a loss storm in the middle of
        # the curve and bend it for reasons that have nothing to do with
        # allocation count.
        ALLOCS_SWEEP="$2"
        shift 2
        ;;
    --no-reset-control-plane)
        # Skip the wipe + reinstall. Only correct when you know the node
        # table already holds exactly this campaign's fleet and nothing
        # else — resuming a campaign that just died mid-step, typically.
        # The per-cell census gate still refuses a contaminated fleet, so
        # the worst case is a loud failure rather than a bad number.
        RESET_CP=false
        shift
        ;;
    --keep-jobs)
        KEEP_JOBS=true
        shift
        ;;
    --submit-concurrency)
        # Jobs submitted in parallel. Raise it when a cell reports
        # submission.driver_bound: that means the driver, not the
        # scheduler, set the measured rate.
        SUBMIT_CONCURRENCY="$2"
        shift 2
        ;;
    --eval-threads)
        # legion-a only. The phase-2 finding was that raising this COST
        # throughput (4 -> 16 threads: -12% at 1k, -11% at 5k) because
        # the extra planners' work was discarded; re-measuring it is how
        # plan-apply-conflict-reduction is judged. One value per
        # campaign — it is a control-plane setting, so sweeping it means
        # one campaign per point.
        EVAL_THREADS="$2"
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

# Refused rather than ignored: only install-legion-a.sh renders this, and
# a silently-dropped --eval-threads would produce a cell archived as a
# 16-thread result whose rendered config says 4.
if [ -n "$EVAL_THREADS" ]; then
    case "$EVAL_THREADS" in
    '' | *[!0-9]*)
        echo "--eval-threads must be a positive integer, got '$EVAL_THREADS'" >&2
        exit 1
        ;;
    esac
    [ "$SYSTEM" = "legion-a" ] || {
        echo "--eval-threads applies to --system legion-a only (got $SYSTEM)" >&2
        exit 1
    }
    $RESET_CP || {
        echo "--eval-threads needs the control-plane reset to apply it; drop --no-reset-control-plane" >&2
        exit 1
    }
    export LEGION_EVAL_THREADS="$EVAL_THREADS"
fi

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

# The allocation densities this campaign runs at each step. One value is
# the ordinary campaign; several is a saturation sweep.
ALLOC_POINTS="${ALLOCS_SWEEP:-$ALLOCS_PER_NODE}"
log "=== campaign $CAMPAIGN: steps [$STEPS], scenarios [$SCENARIOS], allocs/node [$ALLOC_POINTS]${EVAL_THREADS:+, evalThreads $EVAL_THREADS} ==="

# Start from an empty node table.
#
# Nothing removes node records — there is no deregistration RPC, and
# stop-sim-fleet.sh only stops the sim processes — so whatever ran before
# this campaign is still in the store. That is not hypothetical: phase1.sh
# runs the packing-density ladder (up to 4000 simulated nodes) and then a
# campaign, with nothing in between, and the campaign's 1000-node cells
# inherited 3000 orphan rows. The per-cell census gate below now refuses
# to run in that state, so without this reset the campaign would simply
# stop; with it, every campaign begins the way its results claim.
if $RESET_CP; then
    "$(dirname "$0")/reset-control-plane.sh" --system "$SYSTEM"
else
    log "skipping the control-plane reset (--no-reset-control-plane)"
fi

# Bring the sim fleet to (system, scenario, node count), restarting it
# only when it is not already there.
#
# Every restart marks the whole fleet lost and raises a reschedule eval
# per node — 5760 of them against 2503 job submissions at the 10k step —
# and those evals compete with the cell's own work for the same eval
# workers. So a restart is a measurement cost, not just a time cost, and
# the cheapest fix is not paying it when the fleet we want is the fleet
# we already have. That is the common case in a saturation sweep, where
# every point runs at one node count and one scenario.
ensure_sim_fleet() { # ensure_sim_fleet <sim-system> <nodes> <scenario>
    local sim="$1" nodes="$2" scenario="$3" state recorded_slices live_slices manager
    if [ "$sim" = legion ]; then
        state=$(fleet_state)
        recorded_slices=$(jq -r '.slices // empty' <<<"$state")
        manager=$(jq -r '.manager // empty' <<<"$state")
        live_slices=$(worker_instances | grep -c . || true)
        # Reuse only on an exact match of everything that shapes the
        # fleet, AND a census that still holds: a spot reclaim mid-campaign
        # silently shrinks the fleet, and "the state file says 10000" is
        # not evidence. A short timeout — this is a check, not a wait, and
        # its output is silenced because a mismatch here is an ordinary
        # "restart the fleet", not the failure assert_fleet describes. The
        # restart path below runs the same assertion loudly.
        if [ -n "$recorded_slices" ] && [ -n "$manager" ] &&
            [ "$recorded_slices" = "$live_slices" ] &&
            jq -e --arg scenario "$scenario" --argjson nodes "$nodes" \
                '.system == "legion" and .scenario == $scenario and .nodes == $nodes' \
                >/dev/null <<<"$state" 2>/dev/null &&
            assert_fleet "$manager" "$nodes" "$recorded_slices" 30 2>/dev/null; then
            log "reusing the running fleet ($scenario @ ${nodes}n, $recorded_slices slices) — no restart"
            return 0
        fi
        log "fleet is not already $scenario @ ${nodes}n — restarting it"
    fi
    "$(dirname "$0")/stop-sim-fleet.sh"
    "$(dirname "$0")/start-sim-fleet.sh" "$sim" "$nodes" \
        "$SCENARIOS_DIR/$scenario.json"
}

for NODES in $STEPS; do
    INSTANCES=$(((NODES + NODES_PER_INSTANCE - 1) / NODES_PER_INSTANCE))

    log "--- step ${NODES}n: scaling spot fleet to $INSTANCES instance(s) ---"
    scale_workers "$INSTANCES"
    # Spot capacity can take a couple of minutes to arrive.
    wait_for_workers "$INSTANCES" 600
    "$(dirname "$0")/gen-targets.sh"

    # The fleet is shaped from the SCENARIO'S OWN node_profiles, which is
    # why ensure_sim_fleet keys on the scenario as well as the node count
    # (it reuses a fleet only when both already match — several
    # allocation densities at one node count share one fleet, a change of
    # either does not). Reusing one fleet across SCENARIOS is silently
    # fatal, which is what that key exists to prevent: the fleet
    # took `start-sim-fleet.sh`'s default web_fleet shape, so its nodes
    # carried `class: util`, while batch_burst's jobs constrain on
    # `class: batch`. Every one of that cell's evals failed with "no
    # available nodes matching constraints", the driver waited out its
    # full 600s settle timeout, and the cell reported 0/5016 placed --
    # eight of the campaign's twelve cells would have gone that way.
    #
    # Cost of doing it properly: one fleet restart per cell, and because
    # node names embed the profile name (`sim-<i>-<profile>-<n>`), a
    # change of scenario leaves the previous scenario's nodes orphaned in
    # the store. Hence ONE SCENARIO PER CAMPAIGN, with a wiped control
    # plane between campaigns -- there is no node-deregistration surface
    # to clean up with (see TODO.md). Refuse rather than quietly produce
    # a contaminated multi-scenario run.
    SCENARIO_COUNT=$(printf '%s\n' $SCENARIOS | wc -w | tr -d ' ')
    if [ "$SCENARIO_COUNT" -gt 1 ]; then
        echo "run-campaign.sh: --scenarios lists $SCENARIO_COUNT scenarios ($SCENARIOS)." >&2
        echo "  Each needs a fleet shaped from its own node_profiles, and switching" >&2
        echo "  shapes mid-campaign orphans the previous scenario's nodes. Run one" >&2
        echo "  scenario per campaign, wiping the control plane between them:" >&2
        for s in $SCENARIOS; do
            echo "    scripts/run-campaign.sh --system $SYSTEM --scenarios $s" >&2
        done
        echo "  Each of those resets the control plane before it starts, which is" >&2
        echo "  what clears the previous scenario's orphaned node rows." >&2
        exit 1
    fi

    for SCENARIO in $SCENARIOS; do
        ensure_sim_fleet "$SIM_SYSTEM" "$NODES" "$SCENARIO"

        # One cell per allocation density, on the one fleet.
        #
        # Sizing by allocations rather than by a multiplier over each
        # scenario's own base is what makes a cell measurable at all: the
        # bases (180/304/380) were chosen for the local micro-benchmarks,
        # and at that size a step places out in under a second — entirely
        # inside the driver's warmup, so the cell reports zero throughput
        # and zero latency samples. Phase 1's smoke cell did exactly that.
        for ALLOCS in $ALLOC_POINTS; do
            TARGET=$((NODES * ALLOCS))
            log "--- cell: $SCENARIO @ ${NODES}n x ${ALLOCS} allocs/node (~$TARGET allocations) ---"
            "$(dirname "$0")/run-step.sh" \
                --system "$SYSTEM" --nodes "$NODES" --scenario "$SCENARIO" \
                --target-allocations "$TARGET" --warmup "$WARMUP" --campaign "$CAMPAIGN" \
                ${SUBMIT_CONCURRENCY:+--submit-concurrency "$SUBMIT_CONCURRENCY"} \
                --keep-jobs "$KEEP_JOBS"
        done
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
