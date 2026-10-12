#!/usr/bin/env bash
# Automated spot check against the AWS rig. Runs each scenario, prints PASS or
# FAIL with the evidence, and keeps going after a failure.
#
#   tests/spot-check/smoke.sh                 # everything
#   tests/spot-check/smoke.sh container kv    # some scenarios
#
# Scenarios: cluster container exec sandbox kv dag fullstack agents redis failover
# It submits jobs and leaves them running, so you can poke at them afterwards.
set -uo pipefail
source "$(dirname "$0")/infra/env.sh"
# Always the rig's current leader and management token. A LEGION_ADDR already
# in the environment is ignored on purpose: the repo's mise.toml sets it to a
# local manager. Set SC_LEGION_ADDR to aim at a specific manager instead.
eval "$("$SC_DIR/infra/cli-env.sh")"
[ -z "${SC_LEGION_ADDR:-}" ] || export LEGION_ADDR="$SC_LEGION_ADDR"
jobs_dir="$SC_DIR/jobs"
w1="$(sc_public_ip sc-wkr-1)"
w2="$(sc_public_ip sc-wkr-2)"
pass=0
fail=0

ok() { pass=$((pass + 1)); printf '\033[32mPASS\033[0m %s\n' "$*"; }
bad() { fail=$((fail + 1)); printf '\033[31mFAIL\033[0m %s\n' "$*"; }
section() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
# Retries a command until it succeeds or the timeout (seconds) passes.
eventually() {
    local timeout="$1"
    shift
    local end=$((SECONDS + timeout))
    until "$@" >/dev/null 2>&1; do
        [ $SECONDS -ge $end ] && return 1
        sleep 3
    done
}
# `grep -q` exits on the first match; under pipefail the writer's SIGPIPE then
# fails the pipeline. `has` reads all of its input instead.
has() { grep -c -- "$1" >/dev/null; }
http_has() { curl -fsS --max-time 5 "$1" | has "$2"; }
alloc_of() { legion instance get "$1" -j 2>/dev/null | jq -r '.data.instance.current_alloc_id // empty'; }
first_alloc() { # job task -> newest running allocation id of that task
    legion state query "select id from allocation where job_config_id like '$1-%' and task_config_id in (select id from task_config where name = '$2') order by id desc limit 1" -j 2>/dev/null |
        jq -r '.data[0].id // .data.rows[0].id // empty'
}

scenario_cluster() {
    section "cluster"
    local voters
    voters="$(sc_ssh sc-mgr-1 "LEGION_TOKEN=$LEGION_TOKEN legion raft status" 2>/dev/null | grep -oE "^voters [0-9]+" | grep -oE "[0-9]+")"
    [ "${voters:-0}" -ge 3 ] && ok "raft has 3 voters" || bad "raft voters: ${voters:-0}"
    local active
    active="$(legion node list -j | jq '[.data[] | select(.status == "active")] | length')"
    [ "$active" -eq 2 ] && ok "2 workers active" || bad "active workers: $active"
}

scenario_container() {
    section "container (ECR image, static port, logs, exec, action)"
    legion job run "$jobs_dir/01-container.ncl" >/dev/null
    eventually 180 http_has "http://$w1:8081/" '"alloc"' && ok "worker 1 serves :8081" || bad "worker 1 :8081"
    eventually 60 http_has "http://$w2:8081/" '"alloc"' && ok "worker 2 serves :8081" || bad "worker 2 :8081"
    legion job logs hello-container 2>&1 | has '"GET / HTTP' && ok "request lines in job logs" || bad "no request lines in logs"
    legion exec --job hello-container -- cat /etc/alpine-release >/dev/null 2>&1 && ok "exec into container" ||
        bad "exec into container: $(legion exec --job hello-container -- true 2>&1 | tail -1)"
    legion action run hello-container env 2>&1 | has LEGION_ALLOC_ID && ok "action runs" || bad "action env"
}

scenario_exec() {
    section "exec (host process, enforced memory)"
    legion job run "$jobs_dir/02-exec.ncl" >/dev/null
    eventually 120 http_has "http://$w1:8082/" "exec task" && ok "worker 1 serves :8082" || bad "worker 1 :8082"
    eventually 60 http_has "http://$w2:8082/" "exec task" && ok "worker 2 serves :8082" || bad "worker 2 :8082"
    local cg
    cg="$(legion exec --job hello-exec -- sh -c 'cat /sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)/memory.max' 2>&1 | tail -1)"
    [ "$cg" = "$((64 * 1024 * 1024))" ] && ok "memory.max is 64 MiB" || bad "memory.max: $cg"
}

scenario_sandbox() {
    section "sandbox (cloud-hypervisor microVM over TAP)"
    legion job run "$jobs_dir/03-sandbox.ncl" >/dev/null
    if eventually 240 http_has "http://$w1:8090/" "microVM" || eventually 30 http_has "http://$w2:8090/" "microVM"; then
        ok "microVM serves HTTP on :8090"
    else
        bad "microVM not reachable on :8090"
    fi
    legion job logs hello-vm 2>&1 | has "guest kernel" && ok "guest console in job logs" || bad "no guest console output"
}

scenario_kv() {
    section "kv (env restarts, file updates live)"
    legion kv put spot-check/greeting hello >/dev/null
    legion kv put spot-check/message first >/dev/null
    legion job run "$jobs_dir/04-kv.ncl" >/dev/null
    local url started
    for host in "$w1" "$w2"; do eventually 5 http_has "http://$host:8083/" greeting && url="http://$host:8083/"; done
    [ -n "${url:-}" ] || eventually 120 http_has "http://$w1:8083/" greeting && url="${url:-http://$w1:8083/}"
    eventually 60 http_has "$url" '"first"' && ok "initial KV values rendered" || bad "initial render"
    started="$(curl -fsS "$url" | jq -r .started)"
    legion kv put spot-check/message second >/dev/null
    if eventually 60 http_has "$url" '"second"'; then
        [ "$(curl -fsS "$url" | jq -r .started)" = "$started" ] && ok "file updated live, no restart" ||
            bad "file updated but the task restarted"
    else bad "file never updated"; fi
    legion kv put spot-check/greeting bonjour >/dev/null
    eventually 120 http_has "$url" bonjour && ok "env change restarted the task" || bad "env change never applied"
}

scenario_dag() {
    section "dag (fan-out, fan-in, retry, sidecar, HTTP report)"
    local run="smoke-$(date +%s)"
    legion job run "$jobs_dir/05-dag.ncl" --var run="$run" >/dev/null
    local node_ip=""
    if eventually 600 sh -c "curl -fsS --max-time 5 http://$w1:8085/summary.json || curl -fsS --max-time 5 http://$w2:8085/summary.json"; then
        ok "report served on :8085"
        sc_ssh sc-wkr-1 "ls /data/pipeline/run-$run" >/dev/null 2>&1 && node_ip="$w1" || node_ip="$w2"
        curl -fsS "http://$node_ip:8085/summary.json" | has '"flaky_attempts": 2' && ok "flaky step retried once" ||
            bad "flaky attempts: $(curl -fsS "http://$node_ip:8085/summary.json")"
        # A report can still appear if steps start out of order and crash-loop
        # until their inputs exist, so check the order itself.
        local logs early fetches
        logs="$(legion job logs airports-dag 2>&1)"
        early="$(grep -c "FileNotFoundError" <<<"$logs")"
        [ "$early" -eq 0 ] && ok "no step started before its inputs existed" ||
            bad "$early step runs started before their dependencies finished (FileNotFoundError)"
        fetches="$(grep -c "^\[fetch\] fetched" <<<"$logs")"
        [ "$fetches" -eq 1 ] && ok "fetch ran exactly once" || bad "fetch ran $fetches times (a successful batch task restarted)"
    else
        bad "no report after 10 minutes; step logs:"
        legion job logs airports-dag 2>&1 | grep -E "^\[|Error|Traceback" | head -12 | sed 's/^/      /'
    fi
}

scenario_fullstack() {
    section "fullstack (mesh across workers, postgres, cache)"
    # `kv get` exits 0 with data: null for a missing key, so check the data.
    [ "$(legion kv get fullstack/db-password -j | jq -r '.data != null')" = true ] ||
        legion kv put fullstack/db-password "$(openssl rand -hex 16)" >/dev/null
    legion job run "$jobs_dir/06-fullstack.ncl" >/dev/null
    eventually 300 http_has "http://$w1:8080/api/notes" served_by && ok "frontend 1 -> api -> postgres" || bad "frontend 1 api path"
    eventually 60 http_has "http://$w2:8080/api/notes" served_by && ok "frontend 2 -> api -> postgres" || bad "frontend 2 api path"
    local note="smoke-$RANDOM"
    curl -fsS -X POST "http://$w1:8080/api/notes" -d "{\"body\":\"$note\"}" >/dev/null &&
        eventually 30 http_has "http://$w2:8080/api/notes" "$note" && ok "note written via worker 1 is read via worker 2" ||
        bad "note did not round-trip across workers"
    local seen
    seen="$(for _ in $(seq 10); do curl -fsS "http://$w1:8080/api/notes" | jq -r .served_by; done | sort -u | wc -l)"
    [ "$seen" -ge 2 ] && ok "requests balanced across $seen api replicas" || bad "only $seen api replica answered"
}

scenario_agents() {
    section "agents (keyed sandboxes: gated container, microVM)"
    legion job run "$jobs_dir/07-agents.ncl" >/dev/null
    local box="smoke-box-$RANDOM" vm="smoke-vm-$RANDOM" vm_alloc
    # Re-resolve the allocation on every call: it changes across restarts, and
    # allocation ids are reused, so a stale id can point at another job's task.
    box_exec() { local a; a="$(alloc_of "$box")"; [ -n "$a" ] && legion exec "$a" -- "$@"; }
    status_of() { box_exec curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$1" 2>/dev/null; }
    legion instance create agents --task box --id "$box" >/dev/null && ok "box instance created" || bad "box create"
    if eventually 240 box_exec test -f /workspace/journal.md; then
        ok "exec into box; journal on /workspace"
        [ "$(status_of https://pypi.org/simple/)" = 200 ] && ok "egress to pypi.org allowed" || bad "egress to pypi.org blocked"
        [ "$(status_of https://example.com/)" = 200 ] && bad "egress to example.com was allowed" || ok "egress to example.com denied"
        box_exec sh -c 'echo keep-me > /workspace/marker' >/dev/null 2>&1
        legion instance stop "$box" >/dev/null && legion instance start "$box" >/dev/null
        if eventually 300 box_exec grep -q keep-me /workspace/marker; then
            ok "workspace survives stop/start"
        else
            bad "box not back after stop/start: $(legion instance get "$box" 2>&1 | tail -1 | tr -s ' ')"
        fi
    else
        bad "never got a shell in the box"
    fi
    legion instance create agents --task vm --id "$vm" >/dev/null && ok "vm instance created" || bad "vm create"
    eventually 240 sh -c "legion job logs agents 2>&1 | grep -q 'agent $vm up'" && ok "microVM agent booted (console in logs)" ||
        bad "microVM agent never logged"
    vm_alloc="$(alloc_of "$vm")"
    # Capture first: `grep -q` in a pipeline trips pipefail via SIGPIPE.
    local refusal
    refusal="$( [ -n "$vm_alloc" ] && legion exec "$vm_alloc" -- true 2>&1)"
    if grep -q "does not support exec" <<<"$refusal"; then
        ok "exec into microVM refused with a clear error (known gap)"
    else
        bad "exec into microVM: unexpected result"
    fi
}

scenario_redis() {
    section "redis (AOF on the EBS data disk)"
    legion job run "$jobs_dir/08-redis.ncl" >/dev/null
    eventually 180 sh -c "legion action run redis cli-cmd --param cmd='PING' 2>&1 | grep -q PONG" && ok "redis answers" || bad "redis not up"
    legion action run redis cli-cmd --param cmd="SET greeting persisted" >/dev/null 2>&1
    legion job run "$jobs_dir/08-redis.ncl" --force-reschedule >/dev/null
    eventually 180 sh -c "legion action run redis cli-cmd --param cmd='GET greeting' 2>&1 | grep -q persisted" &&
        ok "value survives a reschedule" || bad "value lost on reschedule"
    sc_ssh sc-wkr-1 "test -f /data/redis/appendonlydir/*.manifest || ls /data/redis/appendonlydir" >/dev/null 2>&1 ||
        sc_ssh sc-wkr-2 "ls /data/redis/appendonlydir" >/dev/null 2>&1 && ok "AOF files on /data (EBS)" || bad "no AOF on /data"
}

scenario_failover() {
    section "failover (stop the raft leader; exec and logs via a follower)"
    local leader_ip leader="" new_ip
    leader_ip="${LEGION_ADDR#http://}"
    leader_ip="${leader_ip%:4747}"
    for name in "${SC_MANAGERS[@]}"; do [ "$(sc_public_ip "$name")" = "$leader_ip" ] && leader="$name"; done
    [ -n "$leader" ] || { bad "cannot tell which manager is the leader"; return; }
    sc_ssh "$leader" sudo systemctl stop legion-manager
    sleep 15
    eval "$("$SC_DIR/infra/cli-env.sh")"
    new_ip="${LEGION_ADDR#http://}"
    [ "${new_ip%:4747}" != "$leader_ip" ] && ok "new leader elected (${new_ip%:4747})" || bad "no new leader"
    legion job logs hello-container >/dev/null 2>&1 && ok "logs via new leader" || bad "logs after failover"
    legion exec --job hello-container -- true >/dev/null 2>&1 && ok "exec via new leader" || bad "exec after failover"
    legion job scale hello-exec --task web --count 2 >/dev/null 2>&1 && ok "writes accepted by new leader" || bad "write after failover"
    sc_ssh "$leader" sudo systemctl start legion-manager
    eventually 60 sc_ssh "$leader" "curl -fsS http://127.0.0.1:4749/readyz" && ok "old leader rejoined" || bad "old leader did not rejoin"
}

scenarios=("$@")
[ ${#scenarios[@]} -gt 0 ] || scenarios=(cluster container exec sandbox kv dag fullstack agents redis failover)
for s in "${scenarios[@]}"; do "scenario_$s"; done
printf '\n\033[1m%d passed, %d failed\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
