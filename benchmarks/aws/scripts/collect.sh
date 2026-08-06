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
# The cell's own window when run-step.sh supplies it, and only then a
# trailing 2h superset. The difference decides whether the archived
# series describe THIS cell or the last two hours of rig activity: a 1000
# node cell collected right after a 4000-node packing-density run
# otherwise archives `liveness_tracked_nodes` peaking at 4000 and a
# freshness ratio dipping to 0.009, neither of which happened during the
# cell. Cadence evidence that cannot be attributed to a cell cannot
# certify it, which was the whole reason for capturing it.
#
# A minute of padding each side keeps the fleet ramp and the settle tail
# in frame without reaching back to the previous cell.
START=${CELL_START_EPOCH:-$(date -u -d '2 hours ago' +%s 2>/dev/null || date -u -v-2H +%s)}
[ -n "${CELL_START_EPOCH:-}" ] && START=$((START - 60))
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
# The rate above shows the shape over time; the raw counter gives the
# cell its exact applied/rejected totals as (last - first), which is the
# headline number of the conflict work and the one phase 2 had to
# reconstruct by hand from a whole campaign's cumulative values.
snapshot plan_apply_counter 'sum by (outcome) (legion_plan_apply_total)'
# Two-pass bind (plan-apply-conflict-reduction-plan.md): `clean` bound on
# an unmoved node epoch, `revalidated` had a moved epoch and the
# in-transaction recheck admitted it anyway. `revalidated` is precisely
# the work that used to be thrown away, so it is the fix's own scoreboard.
snapshot plan_bind_pass 'sum by (pass) (legion_scheduler_plan_bind_pass)'
snapshot eval_conflict_requeues 'legion_eval_conflict_requeues_total'
# The raft lanes, which is where phase 3 found the ceiling and could only
# INFER it: plan phases accounted for 41ms/eval while each eval occupied a
# worker for ~298ms, and the missing time is the 3-4 quorum rounds per eval
# that the plan-phase histogram does not cover. These four make that
# directly measurable instead of arithmetic:
#   ops_total     — rounds per lane; divide by evals for rounds/eval
#   pipeline_wait — queueing behind the lane's single pipeline thread
#   pipeline_depth— how deep that queue got
#   propose_apply — the commit itself, once it stops queueing
# Read pipeline_wait against propose_apply: wait >> apply means the lane is
# serialized, not slow, and the fix is fewer ops rather than faster ones.
snapshot raft_ops 'sum by (lane, op, outcome) (legion_raft_ops_total)'
# The top ops by rate on the busiest lane. A lane's op count IS its
# quorum-round count, so this is the list to shorten when a lane is the
# limit. (Phase 3 had no `op` label and the status lane's 411 ops/sec was
# unattributable — it collected `by (lane, op)` against a metric that had
# no such label and silently got the lane totals back.)
snapshot raft_ops_by_op_rate 'topk(15, sum by (lane, op) (rate(legion_raft_ops_total[1m])))'
# Claims that found nothing still cost a full quorum round. A large `empty`
# share means workers are being woken for work that does not exist.
snapshot eval_claims 'sum by (outcome) (legion_eval_claim_total)'
snapshot raft_pipeline_wait_p99 'histogram_quantile(0.99, sum by (lane, le) (rate(legion_raft_pipeline_wait_seconds_bucket[1m])))'
snapshot raft_pipeline_wait_mean 'sum by (lane) (rate(legion_raft_pipeline_wait_seconds_sum[1m])) / sum by (lane) (rate(legion_raft_pipeline_wait_seconds_count[1m]))'
snapshot raft_pipeline_depth 'legion_raft_pipeline_depth'
snapshot raft_propose_apply_mean 'sum by (lane) (rate(legion_raft_propose_apply_seconds_sum[1m])) / sum by (lane) (rate(legion_raft_propose_apply_seconds_count[1m]))'
snapshot store_write_lock_wait_p99 'histogram_quantile(0.99, sum by (le) (rate(legion_store_write_lock_wait_seconds_bucket[1m])))'
snapshot store_read_lock_wait_p99 'histogram_quantile(0.99, sum by (le) (rate(legion_store_read_lock_wait_seconds_bucket[1m])))'
snapshot evals_processed 'sum by (kind, outcome) (legion_evals_processed_total)'
# The group commit, which is the difference between paying plan-apply cost
# per plan and per batch. It only engages when a second eval thread reaches
# the coalescer while a commit is in flight, so the expected batch size is
# ~1 + (arrival rate x commit time) -- about 1.25 at 4 eval threads, i.e.
# effectively off. Read batch_size against evalThreads: if the mean is
# pinned near 1, the batching knob (plan_apply_batch_max, default 16) is
# irrelevant and concurrency is what has to rise.
snapshot coalesce_batch_size_mean 'sum by (path) (rate(legion_store_coalesce_batch_size_sum[1m])) / sum by (path) (rate(legion_store_coalesce_batch_size_count[1m]))'
snapshot coalesce_batch_size_p99 'histogram_quantile(0.99, sum by (path, le) (rate(legion_store_coalesce_batch_size_bucket[1m])))'
snapshot coalesce_wait_mean 'sum by (path, role) (rate(legion_store_coalesce_wait_seconds_sum[1m])) / sum by (path, role) (rate(legion_store_coalesce_wait_seconds_count[1m]))'
snapshot coalesce_commit_mean 'sum by (path) (rate(legion_store_coalesce_commit_seconds_sum[1m])) / sum by (path) (rate(legion_store_coalesce_commit_seconds_count[1m]))'
# Local SQL inside the capture, separated from the quorum wait. capture is
# pure CPU/page-cache (the capture opens and rolls back a savepoint, no
# fsync), so apply_us minus capture minus pipeline_wait is what the raft
# round actually costs.
snapshot raft_capture_mean 'sum by (lane) (rate(legion_raft_capture_seconds_sum[1m])) / sum by (lane) (rate(legion_raft_capture_seconds_count[1m]))'
snapshot heartbeats 'rate(legion_heartbeats_total[1m])'
snapshot nodes 'legion_nodes'
# Whether the fleet actually kept its cadence, archived per cell. Phase 1's
# numbers were unusable for exactly this reason and the archive could not
# show it: the cell measured the heartbeat ceiling rather than the
# scheduler, and nothing in the results said so. The freshness ratio is
# the one number that distinguishes the two (1.0 = every node on time;
# false node loss begins at served_interval/lost_window, 0.20 at the
# defaults), and the suppression counters say whether the manager was
# declining to act on losses it saw. Read these before quoting a cell.
snapshot heartbeat_freshness 'legion_heartbeat_freshness_ratio'
snapshot liveness_tracked_nodes 'legion_liveness_tracked_nodes'
snapshot liveness_loss_suppressed 'sum by (reason) (rate(legion_liveness_loss_suppressed_total[1m]))'
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
# Same window as the range snapshots above, for the same reason: a peak
# taken over a trailing 2h is the rig's peak, not the cell's.
WINDOW_SECS=$((END - START))
[ "$WINDOW_SECS" -lt 60 ] && WINDOW_SECS=60
mem_reduce() { # mem_reduce <name> <promql>
    local fn out
    for fn in max avg; do
        out=$(curl -fsS "$PROM/api/v1/query" \
            --data-urlencode "query=${fn}_over_time(($2)[${WINDOW_SECS}s:15s])" 2>/dev/null) || out=""
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
    # What the archived Prometheus series actually cover. A reader
    # checking a cell's cadence evidence needs to know whether the window
    # is the cell's own or a trailing superset that may include a
    # neighbouring experiment.
    echo "  \"metrics_window\": {"
    echo "    \"start_epoch\": $START, \"end_epoch\": $END,"
    echo "    \"scope\": \"$([ -n "${CELL_START_EPOCH:-}" ] && echo cell || echo "trailing-2h-superset")\""
    echo "  },"
    # The scheduler backlog this cell inherited, from run-step.sh's
    # pre-cell probe. Anything above a handful means the cell shared the
    # eval workers with churn it did not create — a fleet restart marks
    # every node lost and raises a reschedule eval per node, which at 10k
    # nodes outnumbered the cell's own submissions more than two to one.
    # A cell whose throughput looks low is either a real result or this;
    # the number is here so the archive can tell them apart. -1 means the
    # probe could not read the eval table.
    echo "  \"pre_cell_eval_backlog\": ${PRE_CELL_EVAL_DEPTH:--1},"
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
    # Which submission path produced this cell, lifted from the driver's own
    # metrics. `native` is legion-loadgen (one process, pooled connections,
    # concurrent RunJob calls) and is LEGION-ONLY; `cli` is the shared
    # process-per-job loop that Nomad and K8s also use. A cross-system
    # comparison is only valid between cells that agree on this field, so it
    # belongs in the record that makes the numbers defensible and not only in
    # metrics.json. See benchmarks/aws/README.md § Methodology.
    echo "  \"submitter\": $(jq -c '.submitter // "cli"' "$DEST/metrics.json" 2>/dev/null || echo '"unknown"'),"
    echo "  \"legion_binary\": $(aws s3 cp --only-show-errors --region "$REGION" "s3://$BUCKET/bin/legion.meta.json" - 2>/dev/null || echo null),"
    echo "  \"versions\": {"
    echo "    \"nomad\": \"$NOMAD_VERSION\", \"k8s\": \"$K8S_VERSION\","
    echo "    \"kwok\": \"$KWOK_VERSION\", \"nodesim_ref\": \"$NOMAD_NODESIM_REF\""
    echo "  }"
    echo "}"
} >"$DEST/fairness-record.json"
# The exact rendered config the control plane ran with.
rsh "${CP_ADDRS[0]}" "cat /opt/legion/configs/manager.ncl 2>/dev/null || cat /etc/nomad.d/server.hcl 2>/dev/null || sudo cat /etc/kubernetes/kubeadm-config.yaml 2>/dev/null" \
    >"$DEST/control-plane-config" 2>/dev/null || true

log "archiving to s3://$BUCKET/campaigns/$CAMPAIGN/$RUN.tar.gz"
tar -C "$RESULTS_ROOT/$CAMPAIGN" -czf - "$RUN" |
    aws s3 cp --only-show-errors --region "$REGION" - "s3://$BUCKET/campaigns/$CAMPAIGN/$RUN.tar.gz"

# Durable mirror. The rig's own bucket dies with `teardown.sh`
# (force_destroy), and $DEST is on whichever box drove the campaign — for
# an unattended run, a disposable one. A cell that reaches the archive
# bucket survives losing both. Best-effort by design: a mirror failure
# must never fail a cell whose primary copy already landed.
if [ -n "${ARCHIVE_BUCKET:-}" ]; then
    if tar -C "$RESULTS_ROOT/$CAMPAIGN" -czf - "$RUN" |
        aws s3 cp --only-show-errors --region "$REGION" - "s3://$ARCHIVE_BUCKET/campaigns/$CAMPAIGN/$RUN.tar.gz"; then
        log "mirrored to s3://$ARCHIVE_BUCKET/campaigns/$CAMPAIGN/$RUN.tar.gz"
    else
        log "WARNING: mirror to s3://$ARCHIVE_BUCKET failed — this cell only exists in the rig bucket and on this box"
    fi
else
    log "NOTE: no archive_bucket set; this cell dies with teardown.sh unless you sync it out"
fi
log "collected: $DEST"
