#!/usr/bin/env bash
# Packing density: how many simulated nodes one worker instance actually
# carries before it becomes the bottleneck.
#
# The topology table in the plan assumes ~1,250 nodes per c7i.2xlarge.
# That is an estimate, and every scale step is sized from it — so if the
# real ceiling is 2,500, every step needs half the instances, half the
# cost, and half the spot vCPU quota. If it is 800, the campaign has been
# measuring its own sim fleet rather than Legion's managers. Either way
# the number is worth twenty minutes on a rig that is already standing.
#
# Runs an ascending ladder of densities on ONE instance, holds each long
# enough for the rate windows to fill, and reads the fleet's own
# `legion_worker_*` series (docs/metrics.md § Worker) plus node_exporter
# off Prometheus. A density is healthy when the fleet fully registers,
# reports no failures, and sustains its heartbeat cadence with CPU and
# memory headroom to spare.
#
# Usage:
#   packing-density.sh [--steps "1000 2000 4000"] [--hold 180]
#                      [--system legion-a] [--heartbeat-secs 2]
#                      [--campaign <id>] [--scenario <file>] [--no-reset]
#
# Precondition: a Legion control plane installed and running. The probe
# scales the spot fleet to a single instance itself and leaves it there
# — run-campaign.sh sizes the fleet per step anyway, so the next campaign
# reclaims whatever it needs. It also resets that control plane before
# the ladder (--no-reset opts out), because a node table carried over
# from an earlier run would be counted as this drill's fleet.
#
# Safe to run unattended via `remote-campaign.sh exec` (the ladder is
# bounded and every step archives to S3).
set -euo pipefail
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../scripts" && pwd)"
source "$SCRIPTS/env.sh"
load_rig

STEPS="1000 2000 4000" HOLD=180 SYSTEM="legion-a" HEARTBEAT_SECS=2
CAMPAIGN="" SCENARIO="" MANAGER="" RESET=true
while [ $# -gt 0 ]; do
    case "$1" in
    --steps) STEPS="$2" ;;
    --hold) HOLD="$2" ;;
    --system) SYSTEM="$2" ;;
    --heartbeat-secs) HEARTBEAT_SECS="$2" ;;
    --campaign) CAMPAIGN="$2" ;;
    --scenario) SCENARIO="$2" ;;
    --manager)
        # An explicit manager means the caller owns that control plane;
        # do not reinstall something we were only pointed at.
        MANAGER="$2"
        RESET=false
        ;;
    --no-reset)
        RESET=false
        shift
        continue
        ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
    shift 2
done
CAMPAIGN="${CAMPAIGN:-adhoc-$(date -u +%Y%m%d-%H%M%S)}"
SCENARIO="${SCENARIO:-$SCENARIOS_DIR/web_fleet.json}"
MANAGER="${MANAGER:-http://${CP_PRIVATE_IPS[0]}:4747}"

# Exactly one instance, or the ladder measures the wrong thing: with two
# boxes start-sim-fleet.sh halves every density and the ceiling never
# appears. Scaling down is also what makes the probe cheap.
scale_workers 1
wait_for_workers 1

# Refresh the Prometheus node targets for the instance that just came up.
# Without this the file_sd targets still name whatever instance the ASG
# had before, so every `role="sim-worker"` query resolves against a
# terminated box and returns nothing. The drill then prints `cpu ? mem ?`
# and judges saturation on cadence alone — which defeats its entire
# purpose, since the question it exists to answer is whether the SIM HOST
# or the manager is the bottleneck, and CPU/memory headroom is how you
# tell those apart. run-campaign.sh already does this after each scale
# step; this drill scales the fleet itself and has to do the same.
"$SCRIPTS/gen-targets.sh"

mapfile -t WORKERS < <(worker_instances)
[ "${#WORKERS[@]}" -eq 1 ] || {
    echo "expected 1 InService worker instance, found ${#WORKERS[@]}" >&2
    exit 1
}
read -r WORKER_ID _WORKER_PUBLIC _WORKER_PRIVATE <<<"${WORKERS[0]}"

# Start from an empty node table. Nothing deregisters a node, so a
# control plane that has already carried a campaign still holds its rows,
# and the first rung's census would count them as this drill's fleet.
# Skipped with --no-reset or an explicit --manager.
if $RESET; then
    "$SCRIPTS/reset-control-plane.sh" --system "$SYSTEM"
fi

# Ascending, always — but no longer as a workaround.
#
# It used to be one: start-sim-fleet.sh waited on `COUNT(*) >= N`, and
# rows from an earlier, wider rung linger in the store forever, so a
# descending ladder satisfied that wait instantly against stale rows and
# "measured" a fleet that was never started. That hole is closed — the
# wait now asserts an exact census (scripts/env.sh assert_fleet), so a
# descending rung fails loudly and says what it saw instead of quietly
# reporting a density it never reached.
#
# The order is still ascending for two reasons that are about the drill
# rather than about the store: the ladder stops at the first saturated
# rung ("denser steps would only confirm it"), which only means anything
# climbing; and each rung's node names strictly contain the previous
# rung's, so an ascending ladder needs no reset between rungs. If you
# want a descending ladder, reset the control plane between rungs.
STEPS=$(tr ' ' '\n' <<<"$STEPS" | grep -v '^$' | sort -n -u | tr '\n' ' ')

PROM="http://$LOADGEN_ADDR:9090"
promq() { # promq <promql> -> first sample value, or empty
    curl -fsS "$PROM/api/v1/query" --data-urlencode "query=$1" 2>/dev/null |
        jq -r '.data.result[0].value[1] // empty'
}
num() { # num <value> -> JSON number, or null for empty/NaN/Inf
    case "${1:-}" in
    '' | NaN | +Inf | -Inf) echo null ;;
    *) echo "$1" ;;
    esac
}

log "packing density on $WORKER_ID: steps [$STEPS], ${HOLD}s hold each"
log "  a step is healthy at <85% CPU and memory, no failures, and >=95% of cadence"

ROWS="[]"
for N in $STEPS; do
    log "--- $N nodes on one instance ---"
    "$SCRIPTS/stop-sim-fleet.sh" >/dev/null 2>&1 || true

    REGISTERED_OK=true
    "$SCRIPTS/start-sim-fleet.sh" legion "$N" "$SCENARIO" "$MANAGER" || REGISTERED_OK=false
    if [ "$REGISTERED_OK" = false ]; then
        # Denser will not fare better; record the wall and stop climbing.
        log "  fleet did not reach $N registered nodes — treating as the ceiling"
        ROWS=$(jq --argjson nodes "$N" \
            '. + [{nodes: $nodes, healthy: false, reason: "fleet never fully registered"}]' \
            <<<"$ROWS")
        break
    fi

    log "  holding ${HOLD}s for the rate windows to fill"
    sleep "$HOLD"

    REGISTERED=$(promq 'sum(legion_worker_simulated_nodes)')
    TARGET=$(promq 'sum(legion_worker_simulated_nodes_target)')
    HB_OK=$(promq 'sum(rate(legion_worker_heartbeats_total{outcome="ok"}[1m]))')
    HB_BAD=$(promq 'sum(rate(legion_worker_heartbeats_total{outcome!="ok"}[1m]))')
    REG_ERR=$(promq 'sum(rate(legion_worker_simulated_registrations_total{outcome="error"}[1m]))')
    P99=$(promq 'histogram_quantile(0.99, sum by (le) (rate(legion_worker_heartbeat_duration_seconds_bucket[1m])))')
    P50=$(promq 'histogram_quantile(0.50, sum by (le) (rate(legion_worker_heartbeat_duration_seconds_bucket[1m])))')
    CPU=$(promq 'sum(rate(node_cpu_seconds_total{mode!="idle", role="sim-worker"}[1m])) / count(count by (cpu) (node_cpu_seconds_total{role="sim-worker", mode="idle"}))')
    MEM=$(promq '1 - (sum(node_memory_MemAvailable_bytes{role="sim-worker"}) / sum(node_memory_MemTotal_bytes{role="sim-worker"}))')

    ROW=$(jq -n \
        --argjson nodes "$N" \
        --argjson registered "$(num "$REGISTERED")" \
        --argjson target "$(num "$TARGET")" \
        --argjson hb_ok "$(num "$HB_OK")" \
        --argjson hb_failed "$(num "$HB_BAD")" \
        --argjson reg_err "$(num "$REG_ERR")" \
        --argjson p99 "$(num "$P99")" \
        --argjson p50 "$(num "$P50")" \
        --argjson cpu "$(num "$CPU")" \
        --argjson mem "$(num "$MEM")" \
        --argjson hb_secs "$HEARTBEAT_SECS" \
        '{
            nodes: $nodes,
            registered: $registered,
            target: $target,
            heartbeats_ok_per_sec: $hb_ok,
            heartbeats_failed_per_sec: $hb_failed,
            registration_errors_per_sec: $reg_err,
            heartbeat_p50_secs: $p50,
            heartbeat_p99_secs: $p99,
            cpu_utilization: $cpu,
            memory_utilization: $mem,
            # Each node should heartbeat once per interval; the fraction of
            # that cadence the fleet actually sustains is the clearest
            # single signal that the box is keeping up.
            cadence_ratio: (if ($hb_ok != null and $registered != null and $registered > 0)
                            then ($hb_ok / $registered) * $hb_secs else null end)
        }
        | .healthy = (
            .registered != null and .target != null and .registered >= .target
            # Absent means ZERO here, not "assume the worst". Both series
            # are counters selected by a failure label
            # (`outcome!="ok"`, `outcome="error"`), and Prometheus does
            # not create a series until it is first incremented — so a
            # fleet that never failed reports nothing at all. Defaulting
            # those to 1 made a clean run indistinguishable from a broken
            # one, and no step could ever be HEALTHY: the drill reported
            # "no healthy step, even the lowest saturated" on a fleet
            # sitting at 96% cadence and 0.26s p99. Safe because
            # `.measured` below already fails the step outright if the
            # worker telemetry is missing, which is the case `// 1` was
            # reaching for.
            and (.registration_errors_per_sec // 0) == 0
            and (.heartbeats_failed_per_sec // 0) == 0
            and (.cpu_utilization // 1) < 0.85
            and (.memory_utilization // 1) < 0.85
            and (.cadence_ratio // 0) >= 0.95
        )
        # Missing telemetry fails the health test above (via `// 1`),
        # which is the safe default but reports "SATURATED" for what is
        # really "could not measure". They call for opposite responses:
        # saturation means the ceiling was found, unmeasurable means the
        # drill is blind and its answer must not be trusted.
        | .measured = (.cpu_utilization != null and .memory_utilization != null
                       and .cadence_ratio != null)')

    printf '  registered %s/%s  cadence %s  cpu %s  mem %s  p99 %ss  -> %s\n' \
        "$(jq -r '.registered // "?"' <<<"$ROW")" \
        "$(jq -r '.target // "?"' <<<"$ROW")" \
        "$(jq -r 'if .cadence_ratio then (.cadence_ratio * 100 | floor | tostring) + "%" else "?" end' <<<"$ROW")" \
        "$(jq -r 'if .cpu_utilization then (.cpu_utilization * 100 | floor | tostring) + "%" else "?" end' <<<"$ROW")" \
        "$(jq -r 'if .memory_utilization then (.memory_utilization * 100 | floor | tostring) + "%" else "?" end' <<<"$ROW")" \
        "$(jq -r '.heartbeat_p99_secs // "?"' <<<"$ROW")" \
        "$(jq -r 'if .measured | not then "UNMEASURED"
                  elif .healthy then "HEALTHY" else "SATURATED" end' <<<"$ROW")"

    jq -e '.measured' >/dev/null <<<"$ROW" || {
        echo "  telemetry missing for this step — the drill cannot tell a saturated" >&2
        echo "  sim host from a saturated manager, which is the only thing it measures." >&2
        echo "  Check the loadgen Prometheus has live targets for role=\"sim-worker\"" >&2
        echo "  (scripts/gen-targets.sh) and for the legion-sim-workers job." >&2
        exit 1
    }

    ROWS=$(jq --argjson row "$ROW" '. + [$row]' <<<"$ROWS")
    jq -e '.healthy' >/dev/null <<<"$ROW" || {
        log "  saturated at $N — denser steps would only confirm it"
        break
    }
done

"$SCRIPTS/stop-sim-fleet.sh" >/dev/null 2>&1 || true

# The recommendation: the densest healthy step, and what it implies for
# each of the plan's scale steps. `null` means even the first rung was
# saturated — lower the ladder and re-run.
REPORT=$(jq -n \
    --argjson steps "$ROWS" \
    --arg instance "$WORKER_ID" \
    --argjson hb_secs "$HEARTBEAT_SECS" \
    --arg assumed 1250 \
    '($steps | map(select(.healthy)) | max_by(.nodes) | .nodes) as $density
     | {
        measured_at_instance: $instance,
        heartbeat_secs: $hb_secs,
        assumed_nodes_per_instance: ($assumed | tonumber),
        measured_nodes_per_instance: $density,
        steps: $steps,
        implied_fleet: (if $density == null then null else
            [1000, 2000, 5000, 10000, 20000]
            | map({
                nodes: .,
                worker_instances: ((. + $density - 1) / $density | floor),
                spot_vcpu: (((. + $density - 1) / $density | floor) * 8)
              })
        end)
       }')

RUN_ID="experiments/packing-density-$(date -u +%Y%m%d-%H%M%S)"
REMOTE_OUT="/opt/bench/runs/$CAMPAIGN/$RUN_ID"
TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT
jq . <<<"$REPORT" >"$TMP"
rsh "$LOADGEN_ADDR" "mkdir -p '$REMOTE_OUT'"
rcp "$TMP" "$LOADGEN_ADDR:$REMOTE_OUT/metrics.json"

DENSITY=$(jq -r '.measured_nodes_per_instance // "none"' <<<"$REPORT")
"$SCRIPTS/collect.sh" --campaign "$CAMPAIGN" --run "$RUN_ID" --system "$SYSTEM" \
    --nodes "$(jq -r '.measured_nodes_per_instance // 0' <<<"$REPORT")" \
    --scenario packing-density

echo
log "measured density: $DENSITY nodes/instance (plan assumed 1,250)"
log "report: $RESULTS_ROOT/$CAMPAIGN/$RUN_ID/metrics.json"

if [ "$DENSITY" != "none" ]; then
    jq -r '.implied_fleet[] | "  \(.nodes) nodes -> worker_instances=\(.worker_instances) (\(.spot_vcpu) vCPU)"' \
        <<<"$REPORT"
    log "set worker_instances from this table; re-run after any change to the sim path"
    exit 0
fi

# No density means no result, and the whole point of this drill is to
# replace the plan's 1,250 estimate with a measurement. Exiting 0 here
# let an unattended phase-1 run checkpoint the step as "done" while every
# scale step downstream stayed sized from the guess.
#
# Two very different things produce "none", so say which: a ladder where
# every step ran and saturated is a real (if unhelpful) answer, while a
# ladder that produced no samples at all means the drill never executed —
# a missing `legion` on the loadgen box does exactly that.
if [ "$(jq -r '[.steps[]? | select(.registered > 0)] | length' <<<"$REPORT")" = 0 ]; then
    echo "packing density measured nothing: no step registered a single node." >&2
    echo "The ladder did not run — check the log above for the real error" >&2
    echo "(a missing legion CLI on the loadgen box looks exactly like this)." >&2
else
    echo "packing density found no healthy step: even the lowest saturated." >&2
    echo "Re-run with a lower ladder, e.g. --steps \"250 500 1000\"." >&2
fi
exit 1
