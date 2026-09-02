#!/usr/bin/env bash
# The no-AWS dry-run gate from the plan's testing section: prove the
# whole measurement loop — translator -> submission -> event stream ->
# counting math — against a local manager + simulated fleet before any
# cluster-hour is spent. CI-runnable; also the first thing to run when
# the driver changes.
#
#   1. driver + translator fixture tests (pure math, no cluster)
#   2. local manager (local_disk) + `--simulate` fleet over the real wire
#   3. translated web_fleet slice driven by the real driver; asserts
#      every expected allocation is counted as placed
#
# Usage: local-smoke.sh [--nodes 60] [--multiplier 0.1] [--release]
set -euo pipefail

AWS_BENCH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$AWS_BENCH_ROOT/../.." && pwd)"

NODES=60 MULTIPLIER=0.1 PROFILE=debug
while [ $# -gt 0 ]; do
    case "$1" in
    --nodes)
        NODES="$2"
        shift 2
        ;;
    --multiplier)
        MULTIPLIER="$2"
        shift 2
        ;;
    --release)
        PROFILE=release
        shift
        ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
done

log() { echo "[smoke $(date -u +%H:%M:%S)] $*" >&2; }

log "1/3 driver + translator fixture tests"
python3 -m unittest discover "$AWS_BENCH_ROOT/driver/tests" -v 2>&1 | tail -3

# `--features loadgen` builds the native submitter (benchmarks/loadgen)
# alongside the CLI. It is feature-gated so ordinary `cargo build`s do not link
# a second binary, and the smoke test is where its wiring gets exercised: the
# driver run below asserts the cell went through it.
log "building legion ($PROFILE, with plugins — the manager validates the noop runtime against its dylib)"
if [ "$PROFILE" = release ]; then
    (cd "$REPO_ROOT" && LEGION_BUILD_PLUGINS=1 cargo build --release --features loadgen)
else
    (cd "$REPO_ROOT" && LEGION_BUILD_PLUGINS=1 cargo build --features loadgen)
fi
LEGION="$REPO_ROOT/target/$PROFILE/legion"
LOADGEN="$REPO_ROOT/target/$PROFILE/legion-loadgen"

WORK=$(mktemp -d -t legion-smoke-XXXXXX)
MANAGER_PID="" SIM_PID=""
cleanup() {
    [ -n "$SIM_PID" ] && kill "$SIM_PID" 2>/dev/null || true
    [ -n "$MANAGER_PID" ] && kill "$MANAGER_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

log "2/3 local manager + $NODES-node simulated fleet"
mkdir -p "$WORK/manager"
cat >"$WORK/manager/config.ncl" <<EOF
{
  storePath = "$WORK/manager/store",
  workerConfigPath = "$REPO_ROOT/configs/workers/",
  # Same benchmark defaults as the scenario runner: noop tasks with no
  # declared resources must fit anywhere.
  taskResourceDefaults = { cpu = 0, memory = 0, allocations = 1000 },
}
EOF
# exec so $! is the manager itself, not a wrapper subshell — cleanup
# must kill the real process or failed runs leak a manager on :4747.
(cd "$WORK/manager" && exec "$LEGION" start manager -c config.ncl) >"$WORK/manager.log" 2>&1 &
MANAGER_PID=$!
export LEGION_ADDR="http://127.0.0.1:4747"

for _ in $(seq 1 60); do
    curl -fsS -m 2 "http://127.0.0.1:4749/readyz" >/dev/null 2>&1 && break
    kill -0 "$MANAGER_PID" 2>/dev/null || {
        echo "manager died; log:" >&2
        tail -20 "$WORK/manager.log" >&2
        exit 1
    }
    sleep 1
done
curl -fsS -m 2 "http://127.0.0.1:4749/readyz" >/dev/null || {
    echo "manager never became ready" >&2
    exit 1
}

TOKEN=$("$LEGION" join-token create --type default -j | jq -r '.data.token')
[ -n "$TOKEN" ] && [ "$TOKEN" != null ] || {
    echo "failed to mint join token" >&2
    exit 1
}

"$LEGION" start worker "$TOKEN" \
    --simulate "$NODES" \
    --profiles "$REPO_ROOT/benchmarks/scheduler/scenarios/web_fleet.json" \
    --name-prefix smoke \
    --register-rate 500 >"$WORK/sim.log" 2>&1 &
SIM_PID=$!

# `node list` paginates; the state query is the honest fleet count.
count_nodes() {
    "$LEGION" state query 'SELECT COUNT(*) AS n FROM node' -j 2>/dev/null |
        jq -r '.data.rows[0].n // 0' || echo 0
}
for _ in $(seq 1 60); do
    registered=$(count_nodes)
    [ "$registered" -ge "$NODES" ] && break
    sleep 1
done
registered=$(count_nodes)
[ "$registered" -ge "$NODES" ] || {
    echo "fleet stuck at $registered/$NODES nodes; sim log:" >&2
    tail -20 "$WORK/sim.log" >&2
    exit 1
}
log "fleet registered: $registered nodes"

log "3/3 translated workload through the real driver"
python3 "$AWS_BENCH_ROOT/workloads/translate.py" \
    --scenario "$REPO_ROOT/benchmarks/scheduler/scenarios/web_fleet.json" \
    --system legion \
    --count-multiplier "$MULTIPLIER" \
    --out "$WORK/workload"

# --submitter native, not auto: this is the gate that proves the native
# submitter is wired up end to end (built, found, invoked, timestamps rebased
# onto the driver's clock). `auto` would silently fall back to the CLI loop and
# the smoke test would still pass with the path it exists to cover untested.
python3 "$AWS_BENCH_ROOT/driver/driver.py" run \
    --system legion \
    --workload-dir "$WORK/workload" \
    --out "$WORK/run" \
    --manager "$LEGION_ADDR" \
    --legion-bin "$LEGION" \
    --legion-loadgen-bin "$LOADGEN" \
    --submitter native \
    --warmup-secs 0 \
    --settle-timeout-secs 120 |
    tee "$WORK/metrics.json"

COMPLETE=$(jq -r .complete "$WORK/metrics.json")
[ "$COMPLETE" = true ] || {
    echo "driver reported an incomplete run" >&2
    exit 1
}
SUBMITTER=$(jq -r .submitter "$WORK/metrics.json")
[ "$SUBMITTER" = native ] || {
    echo "cell was submitted via '$SUBMITTER', not the native submitter" >&2
    exit 1
}
# Submit stamps must be on the same clock as the placement events, or every
# latency in the cell is nonsense. They come from a different process, rebased
# by --clock-offset-ms, so this is the assertion that keeps that arithmetic
# honest: a submit that appears AFTER the placement it caused means the rebase
# is wrong (a raw epoch stamp would be ~55 years in the future here, which is
# obvious; a sign error would be obvious the other way).
jq -e '.p50_ms >= 0 and .p99_ms < 3600000 and .latency_samples > 0' \
    "$WORK/metrics.json" >/dev/null || {
    echo "implausible latencies — the submitter's clock rebase is wrong:" >&2
    jq '{p50_ms, p95_ms, p99_ms, latency_samples}' "$WORK/metrics.json" >&2
    exit 1
}
log "smoke PASSED: $(jq -r '"\(.placed_total)/\(.expected_total) placed, \(.throughput_per_sec)/s, p95 \(.p95_ms)ms, submitter \(.submitter), submitted \(.submission.rate_per_sec) jobs/s"' "$WORK/metrics.json")"
