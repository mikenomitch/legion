#!/usr/bin/env bash
# 24h Legion soak: mixed+churn at 2k nodes, watching for the slow
# failures a benchmark run can't see — store growth without vacuum,
# eval-queue creep, manager RSS growth, node drift. The workload is
# re-submitted every cycle and a slice of the sim fleet is bounced every
# CHURN_EVERY_SECS, which is the "realistic steady state" from the plan.
#
# Usage: soak-24h.sh [--hours 24] [--campaign <id>]
#   (assumes: Legion A or B installed, sim fleet at 2000 nodes)
set -euo pipefail
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../scripts" && pwd)"
source "$SCRIPTS/env.sh"
load_rig

HOURS=24 CAMPAIGN=""
while [ $# -gt 0 ]; do
    case "$1" in
    --hours) HOURS="$2" ;;
    --campaign) CAMPAIGN="$2" ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
    shift 2
done
CAMPAIGN="${CAMPAIGN:-adhoc-$(date -u +%Y%m%d-%H%M%S)}"
MANAGER="http://${CP_PRIVATE_IPS[0]}:4747"
CHURN_EVERY_SECS=600

RUN_ID="experiments/soak-$(date -u +%Y%m%d-%H%M%S)"
DEST="$RESULTS_ROOT/$CAMPAIGN/$RUN_ID"
mkdir -p "$DEST"
END=$(($(date +%s) + HOURS * 3600))
log "soak: ${HOURS}h of mixed+churn (snapshots to $DEST)"

snapshot() {
    local stamp
    stamp=$(date -u +%FT%TZ)
    {
        printf '{"at":"%s"' "$stamp"
        printf ',"store_bytes":['
        local first=true
        for ip in "${CP_PUBLIC_IPS[@]}"; do
            $first || printf ','
            first=false
            rsh "$ip" "sudo du -sb /var/lib/legion 2>/dev/null | cut -f1" | tr -d '\n' || printf 'null'
        done
        printf ']'
        printf ',"manager_rss_kb":['
        first=true
        for ip in "${CP_PUBLIC_IPS[@]}"; do
            $first || printf ','
            first=false
            rsh "$ip" "ps -o rss= -C legion | head -1" | tr -d ' \n' || printf 'null'
        done
        printf ']'
        local counts
        counts=$(rsh "$LOADGEN_PUBLIC_IP" "LEGION_ADDR=$MANAGER legion state query \
            'SELECT (SELECT COUNT(*) FROM node) AS nodes, (SELECT COUNT(*) FROM allocation) AS allocations, (SELECT COUNT(*) FROM eval) AS evals' -j" |
            jq -c '.data.rows[0]' 2>/dev/null || echo '{}')
        printf ',"counts":%s' "$counts"
        local queue
        queue=$(curl -fsS -m 5 "http://$LOADGEN_PUBLIC_IP:9090/api/v1/query" \
            --data-urlencode 'query=legion_eval_queue_depth' |
            jq -c '[.data.result[].value[1]]' 2>/dev/null || echo '[]')
        printf ',"eval_queue_depth":%s}\n' "$queue"
    } >>"$DEST/snapshots.ndjson"
}

CYCLE=0
while [ "$(date +%s)" -lt "$END" ]; do
    CYCLE=$((CYCLE + 1))
    log "soak cycle $CYCLE"

    # Re-submit the mixed workload (updates are the steady-state load).
    rsh "$LOADGEN_PUBLIC_IP" "
        set -euo pipefail
        mkdir -p /opt/bench/runs/$CAMPAIGN/$RUN_ID/cycle-$CYCLE
        python3 /opt/bench/workloads/translate.py \
            --scenario /opt/bench/scenarios/mixed_heterogeneous.json \
            --system legion --count-multiplier 2 \
            --out /opt/bench/runs/$CAMPAIGN/$RUN_ID/cycle-$CYCLE/workload
        python3 /opt/bench/driver/driver.py run \
            --system legion \
            --workload-dir /opt/bench/runs/$CAMPAIGN/$RUN_ID/cycle-$CYCLE/workload \
            --out /opt/bench/runs/$CAMPAIGN/$RUN_ID/cycle-$CYCLE/run \
            --manager '$MANAGER' --warmup-secs 0 --settle-timeout-secs 600 \
            --legion-bin /usr/local/bin/legion >/dev/null
    " || log "WARNING: cycle $CYCLE workload did not fully converge"

    # Churn: bounce one sim slice (nodes lost + re-registered).
    read -r _ CHURN_PUBLIC _ < <(worker_instances | head -1)
    [ -n "${CHURN_PUBLIC:-}" ] && rsh "$CHURN_PUBLIC" "sudo systemctl restart legion-sim" || true

    snapshot
    sleep "$CHURN_EVERY_SECS"
done

snapshot
log "soak finished after $CYCLE cycles"

# Full-window Prometheus export for the drift analysis.
PROM="http://$LOADGEN_PUBLIC_IP:9090"
for query in legion_eval_queue_depth legion_nodes legion_allocations; do
    curl -fsS "$PROM/api/v1/query_range" \
        --data-urlencode "query=$query" \
        --data-urlencode "start=$((END - HOURS * 3600))" \
        --data-urlencode "end=$END" \
        --data-urlencode "step=300" >"$DEST/prom-$query.json" || true
done

tar -C "$RESULTS_ROOT/$CAMPAIGN" -czf - "$RUN_ID" |
    aws s3 cp --region "$REGION" - "s3://$BUCKET/campaigns/$CAMPAIGN/soak.tar.gz"
log "soak archived. Drift checks: store_bytes monotone growth, eval queue not returning to 0, RSS without plateau."
