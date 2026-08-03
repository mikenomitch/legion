#!/usr/bin/env bash
# Pull one cell's results off the loadgen box, snapshot the relevant
# Prometheus series for its window, write the fairness record, and
# archive everything locally + to S3. The archive IS the methodology:
# nothing gets published that isn't reproducible from one of these.
#
# Usage: collect.sh --campaign <id> --run <run-id> --system <s> \
#            --nodes <n> --scenario <name>
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

CAMPAIGN="" RUN="" SYSTEM="" NODES="" SCENARIO=""
while [ $# -gt 0 ]; do
    case "$1" in
    --campaign) CAMPAIGN="$2" ;;
    --run) RUN="$2" ;;
    --system) SYSTEM="$2" ;;
    --nodes) NODES="$2" ;;
    --scenario) SCENARIO="$2" ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
    shift 2
done
: "${CAMPAIGN:?}" "${RUN:?}" "${SYSTEM:?}" "${NODES:?}" "${SCENARIO:?}"

DEST="$RESULTS_ROOT/$CAMPAIGN/$RUN"
mkdir -p "$DEST"

log "pulling driver output"
rsh "$LOADGEN_ADDR" "tar -C /opt/bench/runs/$CAMPAIGN -czf - $RUN" |
    tar -C "$RESULTS_ROOT/$CAMPAIGN" -xzf -

# Prometheus snapshot: the run's window (from the driver's submit/event
# stamps are monotonic, so use wall-clock now-2h as a safe superset;
# range-export the series the analysis notebook reads).
PROM="http://$LOADGEN_ADDR:9090"
START=$(date -u -d '2 hours ago' +%s 2>/dev/null || date -u -v-2H +%s)
END=$(date -u +%s)
mkdir -p "$DEST/prometheus"
snapshot() { # snapshot <name> <promql>
    curl -fsS "$PROM/api/v1/query_range" \
        --data-urlencode "query=$2" \
        --data-urlencode "start=$START" \
        --data-urlencode "end=$END" \
        --data-urlencode "step=15" \
        >"$DEST/prometheus/$1.json" || log "WARNING: snapshot $1 failed"
}
snapshot plan_phase_p99 'histogram_quantile(0.99, sum by (phase, le) (rate(legion_scheduler_plan_phase_seconds_bucket[1m])))'
snapshot plan_phase_rate 'sum by (phase) (rate(legion_scheduler_plan_phase_seconds_count[1m]))'
snapshot plan_phase_sum 'sum by (phase) (rate(legion_scheduler_plan_phase_seconds_sum[1m]))'
snapshot eval_queue_depth 'legion_eval_queue_depth'
snapshot plan_apply 'sum by (outcome) (rate(legion_plan_apply_total[1m]))'
snapshot heartbeats 'rate(legion_heartbeats_total[1m])'
snapshot nodes 'legion_nodes'
snapshot grpc_p99 'histogram_quantile(0.99, sum by (method, le) (rate(legion_grpc_request_duration_seconds_bucket[1m])))'
snapshot cpu 'sum by (instance_name, role) (rate(node_cpu_seconds_total{mode!="idle"}[1m]))'
# Memory is a first-class result alongside CPU — "what does the control
# plane cost at 1k vs 10k nodes" needs both, and every cell is archived
# at a known node count, so the scale steps are the load axis.
#
# Grouped by instance_name/role to match the cpu snapshot (they were not
# comparable before), and split in two because they answer different
# questions: `used` is what the box needs provisioned, while `anon`
# excludes page cache and is the closer proxy for the manager's own
# footprint — a SQLite-backed store's page cache is a large and growing
# share of `used` that is reclaimable, not resident cost.
snapshot memory_used 'sum by (instance_name, role) (node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes)'
snapshot memory_anon 'sum by (instance_name, role) (node_memory_MemTotal_bytes - node_memory_MemFree_bytes - node_memory_Cached_bytes - node_memory_Buffers_bytes)'

# Reduced memory summary: peak and mean per role, so "memory at low /
# medium / high load" is one small file per cell rather than a
# re-derivation from the range export. The range data above stays
# authoritative; this is the readable form.
#
# Known limitation, stated here because it changes how the number reads:
# this is WHOLE-BOX memory from node_exporter, not the manager process's
# RSS. Legion registers no process collector, so
# `process_resident_memory_bytes` does not exist on :4749. The
# control-plane boxes run nothing else of consequence, so box memory
# tracks the manager closely — but it includes the OS and, in `used`,
# the page cache. Treat these as an upper bound on the manager's cost.
mem_reduce() { # mem_reduce <name> <promql>
    local fn out
    for fn in max avg; do
        out=$(curl -fsS "$PROM/api/v1/query" \
            --data-urlencode "query=${fn}_over_time(($2)[2h:15s])" 2>/dev/null) || out=""
        printf '%s' "${out:-null}" >"$DEST/prometheus/$1-${fn}.json"
    done
}
mem_reduce memory_used_by_role 'sum by (role) (node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes)'
mem_reduce memory_anon_by_role 'sum by (role) (node_memory_MemTotal_bytes - node_memory_MemFree_bytes - node_memory_Cached_bytes - node_memory_Buffers_bytes)'

# Flatten into one cell-level file keyed by role, next to metrics.json,
# so the analysis can read memory the same way it reads throughput.
jq -n \
    --slurpfile used_peak "$DEST/prometheus/memory_used_by_role-max.json" \
    --slurpfile used_mean "$DEST/prometheus/memory_used_by_role-avg.json" \
    --slurpfile anon_peak "$DEST/prometheus/memory_anon_by_role-max.json" \
    --slurpfile anon_mean "$DEST/prometheus/memory_anon_by_role-avg.json" \
    --arg nodes "$NODES" --arg system "$SYSTEM" --arg scenario "$SCENARIO" '
    def series($f): [ ($f[0].data.result // [])[]
                      | {(.metric.role // "unknown"): (.value[1] | tonumber)} ]
                    | add // {};
    {system: $system, scenario: $scenario, nodes: ($nodes | tonumber),
     source: "node_exporter whole-box memory, not process RSS",
     used_peak_bytes: series($used_peak), used_mean_bytes: series($used_mean),
     anon_peak_bytes: series($anon_peak), anon_mean_bytes: series($anon_mean)}' \
    >"$DEST/memory.json" 2>/dev/null || log "WARNING: memory summary failed"

snapshot nomad_plan 'rate(nomad_nomad_plan_apply_count[1m])'
snapshot k8s_sched 'histogram_quantile(0.99, sum by (extension_point, le) (rate(scheduler_framework_extension_point_duration_seconds_bucket[1m])))'

# Fairness record: exactly what ran, where, at what version, with what
# config. This file is what makes the numbers defensible.
log "writing fairness record"
{
    echo "{"
    echo "  \"campaign\": \"$CAMPAIGN\", \"run\": \"$RUN\","
    echo "  \"system\": \"$SYSTEM\", \"nodes\": $NODES, \"scenario\": \"$SCENARIO\","
    echo "  \"collected_at\": \"$(date -u +%FT%TZ)\","
    # From the rig manifest / terraform outputs, never hardcoded: the
    # fairness record has to state the hardware the cell actually ran on.
    echo "  \"instance_types\": {"
    echo "    \"control_plane\": \"$CONTROL_PLANE_TYPE\","
    echo "    \"loadgen\": \"$LOADGEN_TYPE\", \"workers\": \"see terraform/variables.tf\""
    echo "  },"
    # Purchase mix for the sim fleet: an on-demand share means that part
    # of the fleet saw no spot reclaims, which is a fidelity difference
    # between cells and not only a cost one.
    echo "  \"worker_on_demand_percentage\": ${WORKER_ON_DEMAND_PCT:-0},"
    echo "  \"legion_binary\": $(aws s3 cp --region "$REGION" "s3://$BUCKET/bin/legion.meta.json" - 2>/dev/null || echo null),"
    echo "  \"versions\": {"
    echo "    \"nomad\": \"$NOMAD_VERSION\", \"k8s\": \"$K8S_VERSION\","
    echo "    \"kwok\": \"$KWOK_VERSION\", \"nodesim_ref\": \"$NOMAD_NODESIM_REF\""
    echo "  }"
    echo "}"
} >"$DEST/fairness-record.json"
# The exact rendered config the control plane ran with.
rsh "${CP_ADDRS[0]}" "cat /opt/legion/configs/manager.jsonnet 2>/dev/null || cat /etc/nomad.d/server.hcl 2>/dev/null || sudo cat /etc/kubernetes/kubeadm-config.yaml 2>/dev/null" \
    >"$DEST/control-plane-config" 2>/dev/null || true

log "archiving to s3://$BUCKET/campaigns/$CAMPAIGN/$RUN.tar.gz"
tar -C "$RESULTS_ROOT/$CAMPAIGN" -czf - "$RUN" |
    aws s3 cp --region "$REGION" - "s3://$BUCKET/campaigns/$CAMPAIGN/$RUN.tar.gz"

# Durable mirror. The rig's own bucket dies with `teardown.sh`
# (force_destroy), and $DEST is on whichever box drove the campaign — for
# an unattended run, a disposable one. A cell that reaches the archive
# bucket survives losing both. Best-effort by design: a mirror failure
# must never fail a cell whose primary copy already landed.
if [ -n "${ARCHIVE_BUCKET:-}" ]; then
    if tar -C "$RESULTS_ROOT/$CAMPAIGN" -czf - "$RUN" |
        aws s3 cp --region "$REGION" - "s3://$ARCHIVE_BUCKET/campaigns/$CAMPAIGN/$RUN.tar.gz"; then
        log "mirrored to s3://$ARCHIVE_BUCKET/campaigns/$CAMPAIGN/$RUN.tar.gz"
    else
        log "WARNING: mirror to s3://$ARCHIVE_BUCKET failed — this cell only exists in the rig bucket and on this box"
    fi
else
    log "NOTE: no archive_bucket set; this cell dies with teardown.sh unless you sync it out"
fi
log "collected: $DEST"
