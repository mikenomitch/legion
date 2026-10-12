#!/usr/bin/env bash
# The sandbox-scaling campaign: keyed-instance lifecycles (create -> running
# -> suspend -> resume -> destroy) driven against the rig's REAL control
# plane, at a ladder of open-loop rates, plus one density cell that holds
# many sandboxes resident at once. It runs on its own, needs no spot fleet,
# and is the cheap way to put numbers on the sandbox path
# (internal_docs/plans/sandbox-scaling-plan.md, Phase 0 on the AWS rig).
#
#   run-sandbox-campaign.sh --smoke                     # one small rung, ~5 min
#   run-sandbox-campaign.sh                             # the default ladder + density
#   run-sandbox-campaign.sh --rates "100 200 400 800 1600" --density 20000
#
# What it needs: the rig up with no worker fleet at all, which is the cheap
# shape (three control-plane boxes and one load box, about $1.10/hour in
# us-east-1 with the load box at c7i.2xlarge):
#
#   scripts/bringup.sh -var worker_instances=0 -var loadgen_type=c7i.2xlarge
#   scripts/build-and-upload.sh
#   scripts/run-sandbox-campaign.sh
#   scripts/teardown.sh
#
# Why no fleet. `legion-loadgen --mode instances` embeds its simulated
# nodes (checkpoint channel faked) in the load-box process, and they speak
# the real worker protocol over the real network: registration, the
# StreamWork stream, heartbeats, checkpoint reports. The control plane
# under test is the real 3-voter raft cluster the job campaigns measure.
# What is NOT real is the sandbox runtime: boot and restore are simulated,
# so every number here is the control plane's share of a sandbox start,
# which is the part this campaign exists to measure. See
# benchmarks/aws/README.md, "Sandbox-scaling campaign".
#
# Each cell lands in results/<campaign>/sandbox/<cell>.json (the loadgen
# summary) with before/after snapshots of every manager (CPU seconds, RSS,
# applied raft entries, heartbeats), and the campaign ends with
# analysis/sandbox_report.py writing report.md and report.json:
# the ceiling, latencies, raft entries per lifecycle, manager CPU per
# lifecycle, resident density, and labelled projections.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

SYSTEM=legion-a
# Offered sessions/sec per rung, climbing. The ladder stops at the first
# rung that does not hold (unless --no-stop): past the ceiling a rung only
# measures queueing, at full price.
RATES="25 50 100 200 400 800"
# Simulated nodes for the ladder. Capacity is notional (sandbox_nodes.json:
# ~3,000 sandboxes per node), so this sets how many nodes the scheduler and
# the heartbeat path see, not whether the work fits.
SIM_NODES=100
# Sessions per rung: rate x this, clamped. Long enough that each rung runs
# ~20 s at its offered rate, so a ceiling shows as a shortfall rather than
# as a startup transient.
SESSIONS_FACTOR=20
SESSIONS_MIN=200
SESSIONS_MAX=20000
# The density cell: this many sandboxes created at DENSITY_RATE and held
# running for DENSITY_HOLD seconds on DENSITY_NODES nodes (so ~1,000 per
# node at the defaults: a dense node is what the heartbeat path is about).
DENSITY=10000
DENSITY_RATE=250
DENSITY_HOLD=120
DENSITY_NODES=10
SMOKE=false
STOP_AT_CEILING=true
RESET_CP=true
CAMPAIGN="${CAMPAIGN_OVERRIDE:-}"
PROFILES_LOCAL="$AWS_BENCH_ROOT/workloads/sandbox_nodes.json"

usage() {
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
    cat <<'EOF'
Options:
  --system legion-a|legion-b   control plane under test (default legion-a)
  --rates "R1 R2 ..."          offered sessions/sec per rung (default "25 50 100 200 400 800")
  --sim-nodes N                simulated nodes for the ladder (default 100)
  --sessions-max N             cap on sessions per rung (default 20000)
  --density N                  sandboxes held resident in the density cell (0 skips it; default 10000)
  --density-rate R             creation rate for the density cell (default 250)
  --density-hold SECS          how long the density cell holds them (default 120)
  --density-nodes N            nodes the density cell packs onto (default 10)
  --smoke                      one 25/s rung of 200 sessions and a 2,000-sandbox density cell
  --no-stop                    run every rung even past the ceiling
  --no-reset-control-plane     keep the current store (default: wipe and reinstall first)
  --campaign ID                campaign id (default <utc-stamp>-sandbox-<system>)
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
    --system) SYSTEM="$2"; shift 2 ;;
    --rates) RATES="$2"; shift 2 ;;
    --sim-nodes) SIM_NODES="$2"; shift 2 ;;
    --sessions-max) SESSIONS_MAX="$2"; shift 2 ;;
    --density) DENSITY="$2"; shift 2 ;;
    --density-rate) DENSITY_RATE="$2"; shift 2 ;;
    --density-hold) DENSITY_HOLD="$2"; shift 2 ;;
    --density-nodes) DENSITY_NODES="$2"; shift 2 ;;
    --campaign) CAMPAIGN="$2"; shift 2 ;;
    --smoke) SMOKE=true; shift ;;
    --no-stop) STOP_AT_CEILING=false; shift ;;
    --no-reset-control-plane) RESET_CP=false; shift ;;
    -h | --help) usage; exit 0 ;;
    *)
        echo "unknown arg $1" >&2
        usage >&2
        exit 1
        ;;
    esac
done
case "$SYSTEM" in
legion-a | legion-b) ;;
*)
    echo "--system must be legion-a or legion-b (the sandbox path is Legion's)" >&2
    exit 1
    ;;
esac
if $SMOKE; then
    RATES="25"
    SESSIONS_MIN=200
    SESSIONS_MAX=200
    DENSITY=2000
    DENSITY_RATE=200
    DENSITY_HOLD=30
    DENSITY_NODES=2
fi
CAMPAIGN="${CAMPAIGN:-$(date -u +%Y%m%d-%H%M)-sandbox-$SYSTEM}"
OUT="$RESULTS_ROOT/$CAMPAIGN/sandbox"
REMOTE_OUT="/opt/bench/runs/$CAMPAIGN/sandbox"
mkdir -p "$OUT"
log "=== sandbox campaign $CAMPAIGN: $SYSTEM, rates [$RATES], ladder ${SIM_NODES} nodes, density ${DENSITY} on ${DENSITY_NODES} nodes ==="

rsh "$LOADGEN_ADDR" "command -v legion-loadgen >/dev/null" || {
    echo "no legion-loadgen on the load box — run scripts/build-and-upload.sh first" >&2
    exit 1
}

if $RESET_CP; then
    "$(dirname "$0")/reset-control-plane.sh" --system "$SYSTEM"
else
    log "skipping the control-plane reset (--no-reset-control-plane)"
fi

# The manager the load box talks to: the first ready one for raft (any
# voter serves reads and forwards writes), the lease holder for streaming.
find_manager() {
    local ip
    for ip in "${CP_PRIVATE_IPS[@]}"; do
        if rsh "$LOADGEN_ADDR" "curl -fsS -m 3 http://$ip:4749/readyz >/dev/null 2>&1"; then
            echo "http://$ip:4747"
            return 0
        fi
    done
    echo "no ready manager among ${CP_PRIVATE_IPS[*]}" >&2
    return 1
}

# A join token for the embedded fleet's `sim` worker type.
mint_token() {
    local token
    token=$(rsh "$LOADGEN_ADDR" \
        "LEGION_ADDR=$MANAGER legion join-token create --type sim --ttl 24h -j" |
        jq -r '.data.token? // empty' | head -1)
    [ -n "$token" ] || {
        echo "failed to mint a join token (is the sim worker type installed?)" >&2
        return 1
    }
    echo "$token"
}

MANAGER=$(find_manager)
log "manager $MANAGER"
log "minting a sim join token"
TOKEN=$(mint_token)

rsh "$LOADGEN_ADDR" "mkdir -p $REMOTE_OUT"
rcp "$PROFILES_LOCAL" "$LOADGEN_ADDR:$REMOTE_OUT/sandbox_nodes.json"

# One manager's counters, as one JSON object: CPU seconds and RSS of the
# legion process, applied raft entries, heartbeats served.
snapshot_manager() { # snapshot_manager <addr> <name>
    rsh "$1" "
        pid=\$(pgrep -xo legion || true)
        if [ -z \"\$pid\" ]; then echo '{\"name\":\"$2\",\"up\":false}'; exit 0; fi
        tck=\$(getconf CLK_TCK)
        cpu=\$(awk -v t=\"\$tck\" '{printf \"%.2f\", (\$14+\$15)/t}' /proc/\$pid/stat)
        rss=\$(awk '/^VmRSS/{print \$2}' /proc/\$pid/status)
        metrics=\$(curl -fsS -m 5 http://127.0.0.1:4749/metrics || true)
        ops=\$(printf '%s\n' \"\$metrics\" | awk '/^legion_raft_ops_total/ && /outcome=\"applied\"/ {s+=\$NF} END {printf \"%d\", s}')
        hb=\$(printf '%s\n' \"\$metrics\" | awk '/^legion_heartbeats_total /{printf \"%d\", \$NF}')
        vcpu=\$(nproc)
        printf '{\"name\":\"%s\",\"up\":true,\"cpu_secs\":%s,\"rss_kb\":%s,\"raft_applied\":%s,\"heartbeats\":%s,\"vcpus\":%s,\"at\":%s}\n' \
            '$2' \"\$cpu\" \"\${rss:-0}\" \"\${ops:-0}\" \"\${hb:-0}\" \"\$vcpu\" \"\$(date +%s)\"
    "
}

snapshot_all() { # snapshot_all > file.json   (a JSON array, one per manager)
    local i out=()
    for i in "${!CP_ADDRS[@]}"; do
        out+=("$(snapshot_manager "${CP_ADDRS[$i]}" "cp-$i")")
    done
    printf '%s\n' "${out[@]}" | jq -s .
}

# One cell: snapshot, run the loadgen on the load box, snapshot, pull the
# summary back. A density cell also samples the managers every 10 s while it
# holds, so the report can read the resident peak.
run_cell() { # run_cell <cell-id> <rate> <sessions> <hold-ms> <nodes> [sample]
    local id="$1" rate="$2" sessions="$3" hold="$4" nodes="$5" sample="${6:-}" rc=0 sampler=""
    log "--- cell $id: $sessions sessions @ ${rate}/s, hold ${hold} ms, $nodes nodes ---"
    snapshot_all >"$OUT/$id.before.json"
    if [ -n "$sample" ]; then
        (
            while true; do
                snapshot_all | jq -c . >>"$OUT/$id.samples.jsonl" || true
                sleep 10
            done
        ) &
        sampler=$!
    fi
    rsh "$LOADGEN_ADDR" "
        legion-loadgen --mode instances --manager $MANAGER --join-token '$TOKEN' \
            --simulate $nodes --sim-name-prefix sbx --sim-profiles $REMOTE_OUT/sandbox_nodes.json \
            --advertise-addr $LOADGEN_PRIVATE_IP --job sbx-$id \
            --sessions $sessions --rate $rate --hold-ms $hold \
            --stage-timeout-ms 120000 --quiet --out $REMOTE_OUT/$id.json \
            >$REMOTE_OUT/$id.out 2>&1
    " || rc=$?
    [ -n "$sampler" ] && kill "$sampler" 2>/dev/null || true
    snapshot_all >"$OUT/$id.after.json"
    rcp "$LOADGEN_ADDR:$REMOTE_OUT/$id.json" "$OUT/$id.json" 2>/dev/null ||
        rsh "$LOADGEN_ADDR" "tail -40 $REMOTE_OUT/$id.out" >"$OUT/$id.out" 2>&1 || true
    if [ -f "$OUT/$id.json" ]; then
        log "    $(jq -r '"completed \(.completed)/\(.sessions) at \(.sessions_per_sec)/s, create→running p50 \(.stages.create_to_running.p50_ms) ms p99 \(.stages.create_to_running.p99_ms) ms"' "$OUT/$id.json")"
    else
        log "    no summary (rc $rc); see $OUT/$id.out"
    fi
    return "$rc"
}

# Did a rung hold? Every session completed and the achieved rate is within
# 10% of what was offered.
rung_held() { # rung_held <cell-id> <rate>
    [ -f "$OUT/$1.json" ] || return 1
    jq -e --argjson rate "$2" \
        '.failed == 0 and (.sessions_per_sec >= 0.9 * $rate)' "$OUT/$1.json" >/dev/null
}

for rate in $RATES; do
    sessions=$((rate * SESSIONS_FACTOR))
    [ "$sessions" -lt "$SESSIONS_MIN" ] && sessions=$SESSIONS_MIN
    [ "$sessions" -gt "$SESSIONS_MAX" ] && sessions=$SESSIONS_MAX
    id="ladder-r$rate"
    run_cell "$id" "$rate" "$sessions" 0 "$SIM_NODES" || true
    if ! rung_held "$id" "$rate"; then
        log "rung ${rate}/s did not hold"
        if $STOP_AT_CEILING; then
            log "stopping the ladder at the ceiling (--no-stop runs the rest)"
            break
        fi
    fi
done

if [ "$DENSITY" -gt 0 ]; then
    # Every ladder rung re-registered the same `sbx-*` nodes, so they never
    # went lost between rungs. The density cell packs onto fewer nodes, and
    # the rest would go lost all at once — more than half the fleet, which
    # the mass-disruption breaker refuses to mark lost, leaving dead nodes
    # schedulable. A fresh store gives the cell exactly its own nodes.
    if $RESET_CP && [ "$DENSITY_NODES" -ne "$SIM_NODES" ]; then
        "$(dirname "$0")/reset-control-plane.sh" --system "$SYSTEM"
        MANAGER=$(find_manager)
        TOKEN=$(mint_token)
    fi
    run_cell "density-$DENSITY" "$DENSITY_RATE" "$DENSITY" "$((DENSITY_HOLD * 1000))" \
        "$DENSITY_NODES" sample || true
fi

jq -n \
    --arg campaign "$CAMPAIGN" --arg system "$SYSTEM" \
    --arg cp_type "$CONTROL_PLANE_TYPE" --arg lg_type "$LOADGEN_TYPE" \
    --argjson managers "${#CP_ADDRS[@]}" --arg region "$REGION" \
    --arg rates "$RATES" --argjson sim_nodes "$SIM_NODES" \
    --argjson density "$DENSITY" --argjson density_nodes "$DENSITY_NODES" \
    --arg git_rev "$(rsh "$LOADGEN_ADDR" "legion --version 2>/dev/null" | head -1 || true)" \
    --arg at "$(date -u +%FT%TZ)" \
    '{campaign: $campaign, system: $system, control_plane_type: $cp_type,
      control_plane_count: $managers, loadgen_type: $lg_type, region: $region,
      rates: $rates, sim_nodes: $sim_nodes, density: $density,
      density_nodes: $density_nodes, legion_version: $git_rev, finished_at: $at}' \
    >"$OUT/campaign.json"

python3 "$AWS_BENCH_ROOT/analysis/sandbox_report.py" --campaign "$RESULTS_ROOT/$CAMPAIGN" ||
    log "WARNING: sandbox_report.py failed; the raw cells are in $OUT"

aws s3 sync --only-show-errors --region "$REGION" "$RESULTS_ROOT/$CAMPAIGN" \
    "s3://$BUCKET/campaigns/$CAMPAIGN/" || log "WARNING: archive to s3://$BUCKET failed"
if [ -n "${ARCHIVE_BUCKET:-}" ]; then
    aws s3 sync --only-show-errors --region "$REGION" "$RESULTS_ROOT/$CAMPAIGN" \
        "s3://$ARCHIVE_BUCKET/campaigns/$CAMPAIGN/" ||
        log "WARNING: mirror to s3://$ARCHIVE_BUCKET failed"
fi

log "=== sandbox campaign $CAMPAIGN complete ==="
log "report: $RESULTS_ROOT/$CAMPAIGN/sandbox/report.md"
