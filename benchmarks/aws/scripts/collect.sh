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
snapshot memory 'node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes'
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
log "collected: $DEST"
