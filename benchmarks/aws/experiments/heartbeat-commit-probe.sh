#!/usr/bin/env bash
# Heartbeat throughput vs. store commit cost, on ONE box.
#
# Why this exists: the first AWS campaign found a manager that sustained
# only ~80 heartbeat RPCs/sec against a 3-voter raft control plane, with
# nothing saturated — 19-23% manager CPU, 0.2% on the sim hosts, and
# `RaftTransport/AppendEntries` answering in 3ms. The same harness on a
# developer laptop (3 voters on loopback, NVMe) sustains ~2000/sec on a
# SINGLE tokio worker thread. That is a ~100x gap in per-heartbeat cost
# between two setups running identical code.
#
# The variable a laptop cannot reproduce is storage: a raft commit fsyncs,
# and gp3 EBS fsync is orders of magnitude slower than local NVMe. This
# probe isolates that one variable by running the whole thing — every raft
# voter and the simulated fleet — on one instance over loopback. No VPC, no
# spot fleet, no Prometheus, no cross-machine networking. If the ceiling
# reproduces here, it is commit-cost-bound and the fix is amortizing fsync
# (bigger coalescer batches). If it does not, the remaining suspect is the
# multi-machine quorum path, which is a narrower and more expensive
# question to go answer.
#
# Deliberately NOT the benchmark rig: this answers an engineering question,
# not a comparison one, so it needs no identical hardware and no fairness
# record. Running it on the standing operator box costs ~$0.37/hr against
# the rig's ~$2.50/hr.
#
# Usage:
#   heartbeat-commit-probe.sh [--binary PATH] [--nodes "1000 4000"]
#                             [--threads "1 2 4"] [--voters 3] [--hold 60]
#
# Reads: nothing from the rig. Writes: a results table on stdout, plus
# per-run manager logs under a temp dir that is removed on exit.
set -euo pipefail

BINARY=""
NODES_LIST="1000 4000"
THREADS_LIST="1 4"
VOTERS=3
HOLD=60

while [ $# -gt 0 ]; do
    case "$1" in
    --binary) BINARY="$2"; shift 2 ;;
    --nodes) NODES_LIST="$2"; shift 2 ;;
    --threads) THREADS_LIST="$2"; shift 2 ;;
    --voters) VOTERS="$2"; shift 2 ;;
    --hold) HOLD="$2"; shift 2 ;;
    *) echo "unknown arg $1" >&2; exit 1 ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# Prefer an already-built binary; the dist one on a bootstrap box is fine.
if [ -z "$BINARY" ]; then
    for candidate in "$REPO_ROOT/target/dist/legion" "$REPO_ROOT/target/release/legion"; do
        [ -x "$candidate" ] && BINARY="$candidate" && break
    done
fi
[ -n "$BINARY" ] && [ -x "$BINARY" ] || {
    echo "no legion binary found — pass --binary, or build one first" >&2
    exit 1
}

log() { echo "[probe $(date -u +%H:%M:%S)] $*" >&2; }

# Storage characteristics are the thing under test, so state them.
log "binary:  $BINARY"
log "host:    $(uname -m), $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo '?') cpus"
if command -v lsblk >/dev/null 2>&1; then
    log "storage: $(lsblk -no NAME,ROTA,SIZE 2>/dev/null | head -3 | tr '\n' ' ')"
fi

WORK=""
PIDS=()
SIM_PID=""
cleanup_run() {
    [ -n "$SIM_PID" ] && kill "$SIM_PID" 2>/dev/null || true
    for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    PIDS=(); SIM_PID=""
    [ -n "$WORK" ] && rm -rf "$WORK"
}
trap cleanup_run EXIT

# Every voter on loopback. Ports are 4747+10i so a voter's telemetry port
# (4749+10i) never collides with the next voter's gRPC port.
peer_list() {
    local i out=""
    for ((i = 0; i < VOTERS; i++)); do
        out="$out\"cp-$i=127.0.0.1:$((4747 + i * 10))\","
    done
    echo "$out"
}

start_voter() { # start_voter <idx> <bootstrap true|false> <join-token-line>
    local i="$1" bootstrap="$2" join_line="$3"
    local port=$((4747 + i * 10)) tel=$((4749 + i * 10))
    mkdir -p "$WORK/m$i"
    cat >"$WORK/m$i/config.ncl" <<EOF
{
  server = { port = $port, host = "127.0.0.1", advertiseAddr = "http://127.0.0.1:$port" },
  storePath = "$WORK/m$i/store",
  workerConfigPath = "$REPO_ROOT/configs/workers/",
  telemetry = { enabled = true, port = $tel },
  taskResourceDefaults = { cpu = 0, memory = 0, allocations = 1000 },
  persistence = { type = "raft", raft = {
    nodeId = "cp-$i", bootstrap = $bootstrap, $join_line
    advertiseAddr = "127.0.0.1:$port",
    peers = [$(peer_list)],
    expectedVoters = $VOTERS,
    dataDir = "$WORK/m$i/raft",
    topology = "lanes",
  } },
}
EOF
    (cd "$WORK/m$i" && exec "$BINARY" start manager -c config.ncl) \
        >"$WORK/m$i.log" 2>&1 &
    PIDS+=($!)
}

wait_ready() { # wait_ready <telemetry-port> <secs>
    local tel="$1" secs="${2:-90}" i
    for ((i = 0; i < secs; i++)); do
        curl -fsS -m 2 "http://127.0.0.1:$tel/readyz" >/dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}

# One metric value out of the manager's Prometheus endpoint.
metric() { # metric <telemetry-port> <metric-name>
    curl -fsS -m 5 "http://127.0.0.1:$1/metrics" 2>/dev/null |
        awk -v m="$2" '$1 == m {print $2; exit}'
}

# p99 from a Prometheus histogram exposed in text format: walk the buckets
# and return the first upper bound at or above the 99th percentile.
hist_p99() { # hist_p99 <telemetry-port> <bucket-metric> <label-match>
    curl -fsS -m 5 "http://127.0.0.1:$1/metrics" 2>/dev/null |
        grep "^$2" | grep "${3:-}" |
        awk -F'le="' '/le=/ {split($2, a, "\""); v=$0; sub(/.*} /, "", v); print a[1], v}' |
        sort -g | awk '
            { bound[NR] = $1; count[NR] = $2; total = $2 }
            END {
                if (NR == 0 || total == 0) { print "?"; exit }
                target = total * 0.99
                for (i = 1; i <= NR; i++) if (count[i] >= target) { print bound[i]; exit }
                print bound[NR]
            }'
}

run_cell() { # run_cell <threads> <nodes>
    local threads="$1" nodes="$2" i voters_seen="?"
    WORK=$(mktemp -d -t hbprobe-XXXXXX)
    export TOKIO_WORKER_THREADS="$threads"

    start_voter 0 true ""
    wait_ready 4749 90 || { echo "  voter 0 never ready"; tail -5 "$WORK/m0.log" >&2; return 1; }

    export LEGION_ADDR="http://127.0.0.1:4747"
    if [ "$VOTERS" -gt 1 ]; then
        local raft_token
        raft_token=$("$BINARY" raft join-token 2>&1 | grep -o 'lgnraft_[A-Za-z0-9_-]*' | head -1)
        [ -n "$raft_token" ] || { echo "  no raft join token"; return 1; }
        for ((i = 1; i < VOTERS; i++)); do
            start_voter "$i" false "joinToken = \"$raft_token\","
        done
        for ((i = 0; i < 120; i++)); do
            local voters
            voters=$("$BINARY" raft status 2>&1 | grep -oE 'voters [0-9]+' |
                awk '{print $2}' | sort -n | head -1)
            [ "${voters:-0}" -ge "$VOTERS" ] && break
            sleep 2
        done
        voters_seen="${voters:-0}"
    else
        voters_seen=1
    fi

    local token
    token=$("$BINARY" join-token create --type default -j | jq -r '.data.token')
    "$BINARY" start worker "$token" --simulate "$nodes" --name-prefix hb \
        --register-rate 1000 >"$WORK/sim.log" 2>&1 &
    SIM_PID=$!

    local registered=0
    for ((i = 0; i < 180; i++)); do
        registered=$("$BINARY" state query 'SELECT COUNT(*) AS n FROM node' -j 2>/dev/null |
            jq -r '.data.rows[0].n // 0')
        [ "${registered:-0}" -ge "$nodes" ] && break
        sleep 1
    done

    # Let the rate windows fill before sampling, then measure over HOLD.
    sleep 20
    local before after t0 t1
    before=$(metric 4749 legion_heartbeats_total); t0=$(date +%s)
    sleep "$HOLD"
    after=$(metric 4749 legion_heartbeats_total); t1=$(date +%s)

    # The manager's own /metrics is served by the same starved runtime we
    # are measuring: with the worker threads parked in blocking commits it
    # stops answering, and a scrape timeout is indistinguishable from
    # "zero heartbeats". The simulated fleet reports its achieved rate from
    # a process that is NOT saturated, so prefer it whenever the
    # manager-side read came back empty.
    local sim_rate
    # Strip ANSI first: tracing colourises field names, and those escape
    # sequences contain digits, so any digit-based pattern matches inside
    # them instead of the value. `heartbeats_expected_per_sec` is a
    # different key so the anchored match below cannot pick it up.
    sim_rate=$(sed 's/\x1b\[[0-9;]*m//g' "$WORK/sim.log" 2>/dev/null |
        grep -o 'heartbeats_per_sec: "[0-9.]*"' | grep -o '[0-9.]\+' | tail -1)

    local achieved expected pct p99
    local achieved_src=manager
    if [ -n "${before:-}" ] && [ -n "${after:-}" ]; then
        achieved=$(echo "scale=1; ($after - $before) / ($t1 - $t0)" | bc)
    else
        achieved="${sim_rate:-0}"
        log "manager /metrics unreadable (runtime starved?) — using fleet-reported rate"
    fi
    expected=$(echo "scale=1; $nodes / 2" | bc)
    pct=$(echo "scale=0; $achieved * 100 / $expected" | bc 2>/dev/null || echo "?")
    p99=$(hist_p99 4749 legion_grpc_request_duration_seconds_bucket Heartbeat)

    # `registered` and `voters` are reported, not just used internally: a
    # zero throughput row is otherwise uninterpretable. "0/s with 1000
    # nodes registered" is a real ceiling; "0/s with 0 registered" means
    # the fleet never attached and the cell measured nothing at all.
    printf '  %-8s %-7s %-11s %-11s %-7s %-9s %-7s %s\n' \
        "$threads" "$nodes" "$achieved" "$expected" "${pct}%" \
        "${p99}s" "${voters_seen:-?}" "${registered:-0}"

    # Keep the evidence when a cell produced nothing — the temp dir is
    # about to be deleted, and re-running to capture it costs another
    # instance-hour.
    if [ "${achieved%%.*}" = "0" ]; then
        echo "  -- cell produced no heartbeats; manager 0 log tail --" >&2
        tail -20 "$WORK/m0.log" >&2 2>/dev/null || true
        echo "  -- sim log tail --" >&2
        tail -20 "$WORK/sim.log" >&2 2>/dev/null || true
    fi
    cleanup_run
}

echo
echo "  heartbeat throughput vs commit cost — ${VOTERS} voter(s), all on this box"
printf '  %-8s %-7s %-11s %-11s %-7s %-9s %-7s %s\n' threads nodes achieved expected cadence hb_p99 voters registered
printf '  %-8s %-7s %-11s %-11s %-7s %-9s %-7s %s\n' ------- ----- -------- -------- ------- ------ ------ ----------
for threads in $THREADS_LIST; do
    for nodes in $NODES_LIST; do
        run_cell "$threads" "$nodes" || echo "  (cell threads=$threads nodes=$nodes failed)"
    done
done
echo
echo "  Reference points measured 2026-08-03:"
echo "    laptop, NVMe, 3 voters, 1 thread, 4000 nodes -> 1996/s (100% cadence)"
echo "    AWS rig, gp3,  3 voters, 4 vCPU, 1000 nodes  ->   80/s ( 22% cadence)"
echo "  A gp3 box reproducing the rig's number here points at commit cost;"
echo "  matching the laptop instead points at the multi-machine quorum path."
