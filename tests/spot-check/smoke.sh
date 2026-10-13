#!/usr/bin/env bash
# Automated spot check against the AWS rig. Runs each scenario, prints PASS or
# FAIL with the evidence, and keeps going after a failure.
#
#   tests/spot-check/smoke.sh                 # everything
#   tests/spot-check/smoke.sh container kv    # some scenarios
#   tests/spot-check/smoke.sh sandboxes       # every sandbox and session scenario
#
# Scenarios: cluster container exec sandbox kv dag fullstack agents redis
#   sandbox-exec sandbox-net sandbox-suspend sandbox-idle sandbox-store
#   sessions sessions-nodes sessions-restart sessions-edge sandbox-pressure sandbox-preempt
#   sandbox-destroy failover
# Opt-in only (not in the default run): sessions-route, which stops a worker
# for two minutes.
#
# The sandbox and session scenarios are RUNBOOK section 12; read 12.0 for the
# rig changes they need (legion-guest on the workers, edge boot and workload
# identity on the managers, and optionally a contentStore). The ones marked
# "PR #36" test session-durability work that may not be on your branch yet.
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
skipped=0

ok() { pass=$((pass + 1)); printf '\033[32mPASS\033[0m %s\n' "$*"; }
bad() { fail=$((fail + 1)); printf '\033[31mFAIL\033[0m %s\n' "$*"; }
skip() { skipped=$((skipped + 1)); printf '\033[33mSKIP\033[0m %s\n' "$*"; }
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
alloc_of() { legion instance get "$1" -j 2>/dev/null | jq -r '.data.instance.currentAllocId // empty'; }
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
    # The restart can land on the other worker (a new allocation), so accept
    # the answer from either.
    eventually 120 sh -c "curl -fsS --max-time 5 http://$w1:8083/ | grep -q bonjour || curl -fsS --max-time 5 http://$w2:8083/ | grep -q bonjour" &&
        ok "env change restarted the task" || bad "env change never applied"
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
    # Exec into a microVM goes through the guest agent over vsock
    # (docs/task-runtime-sandbox.md, "Exec"). The agent image is musl, so this
    # needs legion-guest beside the worker binary (RUNBOOK 12.0).
    # Capture first: `grep -q` in a pipeline trips pipefail via SIGPIPE.
    local result
    result="$( [ -n "$vm_alloc" ] && legion exec "$vm_alloc" -- uname -r 2>&1)"
    if [ -n "$vm_alloc" ] && legion exec "$vm_alloc" -- true >/dev/null 2>&1; then
        ok "exec into microVM (guest kernel $(tail -1 <<<"$result"))"
    elif grep -q "guest agent" <<<"$result"; then
        bad "exec into microVM: no guest agent in the guest (is legion-guest installed? RUNBOOK 12.0)"
    else
        bad "exec into microVM: $(tail -1 <<<"$result")"
    fi
    # Each run names its own instances, so destroy them or they pile up across
    # runs and starve later scenarios of room.
    legion instance destroy "$box" >/dev/null 2>&1
    legion instance destroy "$vm" >/dev/null 2>&1
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

# --- Sandboxes and sessions (RUNBOOK section 12) ---------------------------
#
# One keyed `ws` sandbox is shared by sandbox-exec, -net, -suspend, and
# -pressure, and destroyed by sandbox-destroy. Ids are never recycled, so each
# run names its own.
hosts_cache="$(sc_hosts)"
name_of_ip() { awk -v ip="$1" '$2 == ip || $3 == ip { print $1 }' <<<"$hosts_cache"; }
pub_of() { awk -v n="$1" '$1 == n { print $2 }' <<<"$hosts_cache"; }
priv_of() { awk -v n="$1" '$1 == n { print $3 }' <<<"$hosts_cache"; }
leader_ip="${LEGION_ADDR#http://}"
leader_ip="${leader_ip%:4747}"
SBX_ID="sbx-smoke-$(date +%s)"
ECHO_KEY="sc-key-$RANDOM$RANDOM"
sandboxes_submitted=""

# A field of `instance get -j` (camelCase InstanceInfo), or nothing.
inst() { legion instance get "$1" -j 2>/dev/null | jq -r ".data.instance.$2 // empty"; }
state_is() { [ "$(inst "$1" state)" = "$2" ]; }
# Run a command in an instance's guest; succeed only if it exited 0.
sbx_ok() {
    local id="$1"
    shift
    [ "$(legion exec --instance "$id" -j -- "$@" 2>/dev/null | jq -r '.data.exitCode // empty')" = 0 ]
}
# A command's stdout from inside an instance's guest.
sbx_run() {
    local id="$1"
    shift
    legion exec --instance "$id" -j -- "$@" 2>/dev/null | jq -r '.data.stdout // empty'
}
# The public URL of an instance's `http` port. The endpoint names the node's
# advertised (private) address, so map it to the box's public one.
sbx_url() {
    local ep name
    ep="$(legion instance get "$1" -j 2>/dev/null |
        jq -r '[.data.instance.endpoints[]? | select(.port == "http")][0] | select(. != null) | "\(.address) \(.number)"')"
    [ -n "$ep" ] || return 1
    name="$(name_of_ip "${ep% *}")"
    echo "http://$(pub_of "$name"):${ep#* }/"
}
# The JSON the guest's server answers with, from the laptop through ingress.
sbx_get() { local url; url="$(sbx_url "$1")" && curl -fsS --max-time 10 "$url"; }
events_has() { # kind text [extra events-list flags...]
    local kind="$1" text="$2" ev
    shift 2
    ev="$(legion events list --kind "$kind" --limit 1000 "$@" -j 2>/dev/null)"
    grep -q -- "$text" <<<"$ev"
}
raft_applied() { # sum of every lane's applied index on the leader
    curl -fsS --max-time 5 "http://$leader_ip:4749/metrics" 2>/dev/null |
        awk '/^legion_raft_applied_index/ { s += $NF } END { printf "%d\n", s }'
}

sandbox_setup() {
    [ -n "$sandboxes_submitted" ] && return 0
    sandboxes_submitted=1
    local w
    for w in "${SC_WORKERS[@]}"; do
        sc_ssh "$w" "test -x /opt/legion/bin/legion-guest" ||
            bad "$w has no /opt/legion/bin/legion-guest: the alpine guests get no exec and no gated network (RUNBOOK 12.0)"
    done
    legion kv put spot-check/echo-key "$ECHO_KEY" >/dev/null
    legion job run "$jobs_dir/09-sandboxes.ncl" >/dev/null || bad "09-sandboxes.ncl was not accepted"
}

ensure_sbx() {
    sandbox_setup
    if [ -z "$(inst "$SBX_ID" state)" ]; then
        legion instance create sandboxes --task ws --id "$SBX_ID" >/dev/null || return 1
    fi
    eventually 300 sbx_ok "$SBX_ID" true
}

scenario_sandbox_exec() {
    section "sandbox-exec (keyed gated microVM: boot, exec through the guest agent, volume)"
    if ! ensure_sbx; then
        bad "no exec session into $SBX_ID within 5 minutes: state $(inst "$SBX_ID" state), $(inst "$SBX_ID" detail)"
        return
    fi
    ok "$SBX_ID booted and takes exec sessions (guest agent over vsock)"
    legion job logs sandboxes 2>&1 | has "sandbox $SBX_ID booted" && ok "guest console in job logs" || bad "no boot line in job logs"
    local code
    code="$(legion exec --instance "$SBX_ID" -j -- sh -c 'exit 3' 2>/dev/null | jq -r '.data.exitCode // empty')"
    [ "$code" = 3 ] && ok "exit status passes through (3)" || bad "exit status: ${code:-none}"
    [ "$(sbx_run "$SBX_ID" sh -c 'echo $LEGION_NETWORK_MODE')" = gated ] && ok "guest env: LEGION_NETWORK_MODE=gated" ||
        bad "guest env has no LEGION_NETWORK_MODE=gated"
    sbx_ok "$SBX_ID" sh -c 'grep -q " /workspace " /proc/mounts' && ok "/workspace is a mounted block volume" ||
        bad "/workspace is not a mount in the guest"
    [ "$(sbx_run "$SBX_ID" sh -c 'ip -o link 2>/dev/null | grep -vc " lo:"')" = 0 ] && ok "no NIC in the guest (gated)" ||
        bad "the gated guest has a network interface"
    legion volume list --job sandboxes -j 2>/dev/null | has "$SBX_ID" && ok "volume bound to $SBX_ID" || bad "no volume listed for $SBX_ID"
}

scenario_sandbox_net() {
    section "sandbox-net (gated egress, host guard, credential injection, ingress)"
    ensure_sbx || { bad "no running $SBX_ID"; return; }
    status() { sbx_run "$SBX_ID" curl -s -o /dev/null -w '%{http_code}' --max-time 15 "$1"; }
    [ "$(status https://pypi.org/simple/)" = 200 ] && ok "allowed: https://pypi.org through the egress proxy" ||
        bad "https://pypi.org was not reachable"
    [ "$(status https://example.com/)" = 200 ] && bad "https://example.com was allowed" ||
        ok "denied: https://example.com (CONNECT refused)"
    local hdrs node_ip
    hdrs="$(sbx_run "$SBX_ID" curl -s -D - -o /dev/null --max-time 15 http://example.com/)"
    grep -qi "^Legion-Denied-By" <<<"$hdrs" && ok "plain-HTTP denial says why ($(grep -i '^Legion-Denied-By' <<<"$hdrs" | tr -d '\r'))" ||
        bad "plain-HTTP denial has no Legion-Denied-By header: $(head -1 <<<"$hdrs")"
    hdrs="$(sbx_run "$SBX_ID" curl -s -D - -o /dev/null --max-time 15 http://169.254.169.254/latest/meta-data/)"
    grep -qi "^Legion-Denied-By: host-guard" <<<"$hdrs" && ok "cloud metadata refused by the host guard, though policy allows it" ||
        bad "metadata: $(head -3 <<<"$hdrs" | tr -d '\r' | tr '\n' ' ')"
    node_ip="$(legion instance get "$SBX_ID" -j 2>/dev/null | jq -r '.data.instance.endpoints[0].address // empty')"
    if [ -n "$node_ip" ]; then
        hdrs="$(sbx_run "$SBX_ID" curl -s -D - -o /dev/null --max-time 15 -x http://127.0.0.1:1080 "http://$node_ip:4848/")"
        grep -qi "^Legion-Denied-By: host-guard" <<<"$hdrs" && ok "the node's own worker port refused by the host guard" ||
            bad "node address $node_ip:4848: $(head -3 <<<"$hdrs" | tr -d '\r' | tr '\n' ' ')"
    fi
    local echo env_dump
    echo="$(sbx_run "$SBX_ID" sh -c 'curl -s --max-time 20 "http://$LEGION_UPSTREAM_ECHO/headers"')"
    grep -q "$ECHO_KEY" <<<"$echo" && ok "the node injected x-spot-check-key on the way to httpbin.org" ||
        bad "no injected header in httpbin's echo: $(head -c 300 <<<"$echo")"
    env_dump="$(sbx_run "$SBX_ID" sh -c 'env; cat /legion/env /legion/resume.env 2>/dev/null')"
    grep -q "$ECHO_KEY" <<<"$env_dump" && bad "the injected key is visible inside the guest" || ok "the key is not in the guest's env"
    local rotated="sc-key-rotated-$RANDOM"
    legion kv put spot-check/echo-key "$rotated" >/dev/null
    ECHO_KEY="$rotated"
    if eventually 60 sh -c "legion exec --instance $SBX_ID -j -- sh -c 'curl -s --max-time 10 http://\$LEGION_UPSTREAM_ECHO/headers' 2>/dev/null | grep -q $rotated"; then
        ok "a KV rotation reaches the node with no restart"
    else
        bad "the rotated key never reached the injection"
    fi
    local url
    url="$(sbx_url "$SBX_ID")"
    [ -n "$url" ] && eventually 60 http_has "$url" '"process"' && ok "ingress: $url answers (datapath, vsock into the guest)" ||
        bad "ingress: no answer at ${url:-<no http endpoint in instance get>}"
    grep -qi deny <<<"$(legion net policy test sandboxes --task ws --to https://example.com/ 2>&1)" && ok "net policy test explains the deny" ||
        bad "net policy test did not say deny for example.com"
}

scenario_sandbox_suspend() {
    section "sandbox-suspend (memory checkpoint, resume in place, cold boot, stop/start)"
    ensure_sbx || { bad "no running $SBX_ID"; return; }
    local marker="m-$RANDOM" out before after boots
    sbx_ok "$SBX_ID" sh -c "echo $marker > /tmp/marker" || bad "could not write /tmp/marker"
    before="$(sbx_get "$SBX_ID" | jq -r '.process // empty')"
    out="$(legion instance suspend "$SBX_ID" --wait -j 2>&1)"
    if [ "$(jq -r '.data.state // empty' <<<"$out" 2>/dev/null)" = suspended ]; then
        ok "suspended: $(jq -r '"\(.data.kind) checkpoint, \(.data.bytes) bytes, \(.data.durationMs) ms on \(.data.node)"' <<<"$out")"
    else
        bad "suspend: $(tail -3 <<<"$out" | tr '\n' ' ')"
        return
    fi
    [ "$(inst "$SBX_ID" suspendReason)" = manual ] && ok "suspend reason manual" || bad "suspend reason: $(inst "$SBX_ID" suspendReason)"
    out="$(legion instance start "$SBX_ID" --wait -j 2>&1)"
    if [ "$(jq -r '.data.coldBoot // empty' <<<"$out" 2>/dev/null)" = "" ] && [ "$(jq -r '.data.restoreMs // empty' <<<"$out" 2>/dev/null)" != "" ]; then
        ok "resumed from memory in $(jq -r .data.restoreMs <<<"$out") ms"
    else
        bad "resume: $(tail -3 <<<"$out" | tr '\n' ' ')"
    fi
    eventually 120 sbx_ok "$SBX_ID" grep -q "$marker" /tmp/marker && ok "/tmp (guest RAM) survived: a memory restore, not a reboot" ||
        bad "/tmp/marker is gone after resume"
    sbx_ok "$SBX_ID" test -f /tmp/resumed-at && ok "suspend.onResume ran in the guest" || bad "no /tmp/resumed-at: onResume did not run"
    after="$(eventually 60 sbx_get "$SBX_ID" && sbx_get "$SBX_ID" | jq -r '.process // empty')"
    [ -n "$before" ] && [ "$after" = "$before" ] && ok "the same server process answers after resume ($after)" ||
        bad "server process before $before, after ${after:-none}"
    sbx_run "$SBX_ID" cat /legion/resume.env | has LEGION_ALLOC_ID && ok "/legion/resume.env names the new allocation" ||
        bad "no /legion/resume.env after the restore"
    legion instance stop "$SBX_ID" >/dev/null && legion instance start "$SBX_ID" >/dev/null
    if eventually 300 sbx_ok "$SBX_ID" true; then
        sbx_ok "$SBX_ID" test -f /tmp/marker && bad "/tmp survived stop/start (expected a cold boot)" || ok "stop/start is a cold boot: /tmp is fresh"
        boots="$(sbx_run "$SBX_ID" sh -c 'wc -l < /workspace/boots' | tr -d ' ')"
        [ "${boots:-0}" -ge 2 ] && ok "/workspace kept across the cold boot ($boots boots)" || bad "/workspace/boots: ${boots:-missing}"
    else
        bad "$SBX_ID did not come back after stop/start"
    fi
    legion instance suspend "$SBX_ID" --wait >/dev/null 2>&1
    out="$(legion instance start "$SBX_ID" --cold --wait -j 2>&1)"
    eventually 300 sbx_ok "$SBX_ID" true && ok "start --cold boots fresh from a suspended state" ||
        bad "start --cold: $(tail -2 <<<"$out" | tr '\n' ' ')"
}

scenario_sandbox_idle() {
    section "sandbox-idle (the idle policy suspends; the gateway wakes it on a request)"
    sandbox_setup
    local id="idle-smoke-$(date +%s)" before out mgr t0
    legion instance create sandboxes --task idler --id "$id" >/dev/null || { bad "create $id"; return; }
    eventually 300 sbx_ok "$id" true || { bad "$id never took an exec session"; legion instance destroy "$id" >/dev/null 2>&1; return; }
    eventually 60 sbx_get "$id" >/dev/null
    before="$(sbx_get "$id" | jq -r '.process // empty')"
    # Hands off now: no exec, no traffic. Polling the manager is not activity.
    if eventually 240 state_is "$id" suspended; then
        ok "suspended by the idle policy (reason $(inst "$id" suspendReason))"
    else
        bad "$id still $(inst "$id" state) after 4 minutes quiet (idle since $(inst "$id" idleSince); detail $(inst "$id" detail))"
        legion instance destroy "$id" >/dev/null 2>&1
        return
    fi
    # The gateway dials the instance's node on its dynamic port, which the rig
    # opens between workers, so run it on a worker.
    mgr="$(priv_of "$(name_of_ip "$leader_ip")")"
    # Two calls: `pkill -f` would match the remote shell's own command line,
    # which also contains the gateway command, and kill it before nohup runs.
    sc_ssh sc-wkr-2 "pkill -f '[l]egion start gateway'"
    sc_ssh sc-wkr-2 "nohup env LEGION_ADDR=http://$mgr:4747 LEGION_TOKEN=$LEGION_TOKEN legion start gateway --listen 127.0.0.1:4780 >/tmp/sc-gateway.log 2>&1 </dev/null &"
    sleep 3
    t0=$SECONDS
    out="$(sc_ssh sc-wkr-2 "curl -s --max-time 90 -H 'Legion-Instance: $id' http://127.0.0.1:4780/")"
    if jq -e '.process' >/dev/null 2>&1 <<<"$out"; then
        ok "the gateway woke $id and answered in $((SECONDS - t0)) s"
        [ "$(jq -r .process <<<"$out")" = "$before" ] && ok "same server process after the wake (memory restore)" ||
            bad "process changed across the wake: $before -> $(jq -r .process <<<"$out")"
    else
        bad "gateway answer: ${out:-nothing}; log: $(sc_ssh sc-wkr-2 tail -2 /tmp/sc-gateway.log | tr '\n' ' ')"
    fi
    sc_ssh sc-wkr-2 "pkill -f '[l]egion start gateway'"
    legion instance destroy "$id" >/dev/null 2>&1
}

scenario_sandbox_store() {
    section "sandbox-store (durability store: publish, then resume on the other worker)"
    if ! legion node info sc-wkr-1 -j 2>/dev/null | has content-publish; then
        skip "sc-wkr-1 does not advertise content-publish: add a contentStore to the worker config (RUNBOOK 12.0)"
        return
    fi
    legion job run "$jobs_dir/10-sandbox-store.ncl" >/dev/null || { bad "10-sandbox-store.ncl was not accepted"; return; }
    local id="sbs-smoke-$(date +%s)" marker="m-$RANDOM" out node new
    legion instance create sandbox-store --id "$id" >/dev/null || { bad "create $id"; return; }
    eventually 300 sbx_ok "$id" true || { bad "$id never took an exec session"; return; }
    sbx_ok "$id" sh -c "echo $marker > /tmp/marker"
    out="$(legion instance suspend "$id" --wait -j 2>&1)"
    [ "$(jq -r '.data.durability // empty' <<<"$out" 2>/dev/null)" = store ] && ok "suspended with durability store" ||
        { bad "suspend: $(tail -3 <<<"$out" | tr '\n' ' ')"; return; }
    if eventually 300 sh -c "[ \"\$(legion instance get $id -j 2>/dev/null | jq -r .data.instance.checkpointState)\" = published ]"; then
        ok "checkpoint published to the content store"
    else
        bad "checkpoint still $(inst "$id" checkpointState): $(inst "$id" detail)"
        return
    fi
    node="$(inst "$id" checkpointNodeId)"
    legion node cordon "$node" >/dev/null
    out="$(legion instance start "$id" --wait --timeout 300 -j 2>&1)"
    new="$(jq -r '.data.node // empty' <<<"$out" 2>/dev/null)"
    [ -n "$new" ] && [ "$new" != "$node" ] && ok "resumed on $new (taken on $node), restore $(jq -r .data.restoreMs <<<"$out") ms" ||
        bad "resume: node ${new:-none} (taken on $node): $(tail -2 <<<"$out" | tr '\n' ' ')"
    eventually 120 sbx_ok "$id" grep -q "$marker" /tmp/marker && ok "guest memory moved with the checkpoint" ||
        bad "/tmp/marker missing after the cross-node resume"
    legion node uncordon "$node" >/dev/null
    legion instance destroy "$id" >/dev/null 2>&1
}

scenario_sessions() {
    section "sessions (start with no raft write, list, burst, promote, end)"
    legion job run "$jobs_dir/11-sessions.ncl" >/dev/null || { bad "11-sessions.ncl was not accepted"; return; }
    local out id before after results started refused other listed tags
    first_session() { out="$(legion instance create sessions --task shell -j 2>/dev/null)" && [ "$(jq -r .ok <<<"$out")" = true ]; }
    eventually 180 first_session || { bad "no session started in 3 minutes: $out"; return; }
    id="$(jq -r '.data.instanceIds[0]' <<<"$out")"
    [ "$(jq -r '"\(.data.mode) \(.data.durable)"' <<<"$out")" = "session false" ] &&
        ok "started session $id on $(jq -r .data.node <<<"$out") (mode session, not durable)" || bad "create: $out"
    before="$(raft_applied)"
    results="$(seq 24 | xargs -P 24 -I{} sh -c 'legion instance create sessions --task shell -j 2>/dev/null | jq -c "{ok, code: .error.code}"')"
    after="$(raft_applied)"
    started="$(grep -c '"ok":true' <<<"$results")"
    refused="$(grep -c 'RESOURCE_EXHAUSTED' <<<"$results")"
    other=$((24 - started - refused))
    [ "$other" -eq 0 ] && ok "burst of 24 concurrent starts: $started started, $refused refused as RESOURCE_EXHAUSTED" ||
        bad "burst of 24: $started started, $refused refused, $other other errors: $(grep -v '"ok":true' <<<"$results" | sort | uniq -c | head -3 | tr '\n' ' ')"
    [ -n "$before" ] && [ $((after - before)) -lt "$started" ] && ok "raft applied index moved $((after - before)) over $started starts (not one per start)" ||
        bad "raft applied index moved $((after - before)) over $started starts"
    out="$(legion instance list sessions -j 2>/dev/null)"
    listed="$(jq '[.data.instances[] | select(.mode == "session")] | length' <<<"$out")"
    [ "$(jq -r .data.complete <<<"$out")" = true ] && [ "$listed" -ge $((started + 1)) ] &&
        ok "list: $listed sessions, complete, asOf $(jq -r .data.asOf <<<"$out")" ||
        bad "list: $listed sessions, complete $(jq -r .data.complete <<<"$out")"
    # PR #36 Phase 3: a session minted by a router or a node carries its
    # node's tag. Ids derived from an idempotency key carry none.
    tags="$(jq -r '.data.instances[] | select(.mode == "session") | "\(.nodeId) \((.instanceId | capture("-(?<t>[a-z2-7]{8})[.]").t?) // "untagged")"' <<<"$out" | grep -v untagged | sort -u)"
    if [ -z "$tags" ]; then
        skip "session ids carry no node tag (PR #36 Phase 3 not on this branch)"
    elif [ "$(awk '{print $1}' <<<"$tags" | sort | uniq -d | wc -l)" -eq 0 ]; then
        ok "one node tag per node in the session ids ($(tr '\n' ' ' <<<"$tags"))"
    else
        bad "a node's sessions carry more than one tag: $(tr '\n' ' ' <<<"$tags")"
    fi
    out="$(legion instance create sessions --task vm -j 2>/dev/null)"
    local vm
    vm="$(jq -r '.data.instanceIds[0] // empty' <<<"$out")"
    [ -n "$vm" ] && eventually 180 state_is "$vm" active && ok "microVM session $vm active" ||
        bad "microVM session: ${vm:-not started} $(inst "$vm" state) $(jq -c .error <<<"$out" 2>/dev/null)"
    local named="promoted-$(date +%s)"
    out="$(legion instance promote "$id" --id "$named" -j 2>&1)"
    [ "$(jq -r '.data.durable // empty' <<<"$out" 2>/dev/null)" = true ] && ok "promoted $id to $named (allocation $(jq -r .data.allocationId <<<"$out"))" ||
        bad "promote: $(tail -2 <<<"$out" | tr '\n' ' ')"
    eventually 60 sh -c "legion exec --instance $named -- true >/dev/null 2>&1" && ok "exec into the promoted instance" ||
        bad "exec into the promoted instance $named"
    legion instance destroy "$named" >/dev/null 2>&1
    legion instance stop "$vm" >/dev/null 2>&1
    eventually 60 sh -c "! legion instance get $vm -j >/dev/null 2>&1" && ok "stopped session $vm is gone" ||
        bad "session $vm still answers after stop: $(inst "$vm" state)"
}

scenario_sessions_restart() {
    section "sessions-restart (a worker restart re-adopts its sessions from sessions.db; PR #36)"
    legion job run "$jobs_dir/11-sessions.ncl" >/dev/null
    local i node ids before after pids_before pids_after missing=0 id
    for i in 1 2 3 4 5 6; do legion instance create sessions --task shell >/dev/null 2>&1; done
    node="$(legion instance list sessions -j 2>/dev/null |
        jq -r '[.data.instances[] | select(.mode == "session" and (.instanceId | startswith("shell-")))] | group_by(.nodeId) | max_by(length) | .[0].nodeId // empty')"
    [ -n "$node" ] || { bad "no shell sessions running"; return; }
    ids="$(legion instance list sessions -j 2>/dev/null |
        jq -r --arg n "$node" '.data.instances[] | select(.mode == "session" and .nodeId == $n and (.instanceId | startswith("shell-"))) | .instanceId' | sort)"
    pids_before="$(sc_ssh "$node" "pgrep -x -f 'sleep 86399' | sort")"
    sc_ssh "$node" "sudo find /var/lib/legion -name sessions.db" | has sessions.db && ok "$node keeps a sessions.db" ||
        skip "no sessions.db on $node (PR #36 Phase 2 not on this branch)"
    sc_ssh "$node" sudo systemctl restart legion-worker
    sleep 10
    if eventually 180 sh -c "legion instance list sessions -j 2>/dev/null | jq -e .data.complete >/dev/null"; then
        after="$(legion instance list sessions -j 2>/dev/null | jq -r '.data.instances[].instanceId' | sort)"
        for id in $ids; do grep -qx "$id" <<<"$after" || missing=$((missing + 1)); done
        [ "$missing" -eq 0 ] && ok "all $(wc -w <<<"$ids") sessions on $node listed again after the restart" ||
            bad "$missing of $(wc -w <<<"$ids") sessions on $node gone after the worker restart"
    else
        bad "listing never complete after restarting $node"
    fi
    pids_after="$(sc_ssh "$node" "pgrep -x -f 'sleep 86399' | sort")"
    [ -n "$pids_before" ] && [ "$pids_before" = "$pids_after" ] && ok "the same session processes (re-adopted, not restarted)" ||
        bad "session processes changed across the restart: before $(wc -w <<<"$pids_before"), after $(wc -w <<<"$pids_after")"
}

scenario_sessions_nodes() {
    section "sessions-nodes (legion session nodes: tags, accepting, ids carry their node's tag; PR #36)"
    legion job run "$jobs_dir/11-sessions.ncl" >/dev/null
    local nodes out id node tag want
    nodes="$(legion session nodes sessions --task shell -j 2>/dev/null)"
    if [ "$(jq -r '.ok' <<<"$nodes" 2>/dev/null)" != true ]; then
        bad "session nodes: $(jq -c .error <<<"$nodes" 2>/dev/null || echo "$nodes" | tail -1)"
        return
    fi
    [ "$(jq '[.data.nodes[] | select(.tag | test("^[a-z2-7]{8}$"))] | length' <<<"$nodes")" -eq "${#SC_WORKERS[@]}" ] &&
        ok "every worker listed with an 8-character tag: $(jq -r '[.data.nodes[] | "\(.node)=\(.tag)"] | join(" ")' <<<"$nodes")" ||
        bad "session nodes: $(jq -c '[.data.nodes[] | {node, tag}]' <<<"$nodes")"
    [ "$(jq '[.data.nodes[] | select(.accepting)] | length' <<<"$nodes")" -eq "${#SC_WORKERS[@]}" ] &&
        ok "every worker accepting edge starts (edge-sessions advertised)" ||
        bad "not accepting: $(jq -c '[.data.nodes[] | select(.accepting | not) | {node, reason}]' <<<"$nodes") (edgeBoot.acceptClaims: RUNBOOK 12.0)"
    out="$(legion instance create sessions --task shell -j 2>/dev/null)"
    id="$(jq -r '.data.instanceIds[0] // empty' <<<"$out")"
    node="$(jq -r '.data.node // empty' <<<"$out")"
    tag="$(sed -nE 's/^.*-([a-z2-7]{8})[.].*$/\1/p' <<<"$id")"
    want="$(jq -r --arg n "$node" '.data.nodes[] | select(.node == $n) | .tag' <<<"$nodes")"
    [ -n "$tag" ] && [ "$tag" = "$want" ] && ok "$id landed on $node and carries its tag $tag" ||
        bad "session ${id:-none} on ${node:-?}: tag ${tag:-none}, $node's tag ${want:-unknown}"
    # Tag routing: a tagged id's get and stop go to its node first.
    legion instance get "$id" -j >/dev/null 2>&1 && ok "instance get routes $id to its node" || bad "instance get $id"
    legion instance stop "$id" >/dev/null 2>&1 && ok "instance stop routes $id to its node" || bad "instance stop $id"
}

scenario_sessions_edge() {
    section "sessions-edge (session ticket; legion session start/get/stop at a worker, no manager; PR #36)"
    legion job run "$jobs_dir/11-sessions.ncl" >/dev/null
    local ticket nodes a1 a2 out sid key a b
    ticket="$(legion boot-ticket create sessions --task shell --pool default --scope session --ttl 30m -j 2>/dev/null | jq -r '.data.ticket // empty')"
    if [ -z "$ticket" ]; then
        bad "could not mint a session ticket: $(legion boot-ticket create sessions --task shell --pool default --scope session --ttl 30m 2>&1 | tail -1) (edgeBoot and workloadIdentity on the managers: RUNBOOK 12.0)"
        return
    fi
    ok "minted a session-scope ticket"
    nodes="$(legion session nodes sessions --task shell -j 2>/dev/null)"
    a1="$(jq -r '.data.nodes[] | select(.node == "sc-wkr-1") | .address' <<<"$nodes")"
    a2="$(jq -r '.data.nodes[] | select(.node == "sc-wkr-2") | .address' <<<"$nodes")"
    [ -n "$a1" ] && [ -n "$a2" ] || { bad "session nodes gave no worker addresses: $(jq -c .data.nodes <<<"$nodes")"; return; }
    # Port 4848 only accepts the managers' security group, so run the edge
    # commands on sc-mgr-1 (its legion fetches the cluster CA with the token).
    edge() { # legion session args..., run on sc-mgr-1
        sc_ssh sc-mgr-1 "LEGION_ADDR=http://127.0.0.1:4747 LEGION_TOKEN=$LEGION_TOKEN legion session $* -j" 2>&1
    }
    out="$(edge start sessions --task shell --node "$a1" --node-name sc-wkr-1 --ticket "$ticket")"
    sid="$(jq -r '.data.sessionId // empty' <<<"$out" 2>/dev/null)"
    [ -n "$sid" ] && ok "edge start on sc-wkr-1: $sid ($(jq -r .data.state <<<"$out"))" || { bad "session start: $(tr '\n' ' ' <<<"$out")"; return; }
    out="$(edge get "$sid" --node "$a1" --ticket "$ticket")"
    [ "$(jq -r '.data.origin // empty' <<<"$out" 2>/dev/null)" = edge ] && ok "session get: origin edge, state $(jq -r .data.state <<<"$out")" ||
        bad "session get: $(tr '\n' ' ' <<<"$out")"
    eventually 30 sh -c "legion instance get $sid -j >/dev/null 2>&1" && ok "the managers find it (routed by its tag)" ||
        bad "no manager knows $sid after 30 s"
    out="$(edge stop "$sid" --node "$a2" --ticket "$ticket")"
    grep -q wrong_node <<<"$out" && ok "sc-wkr-2 answers wrong_node for sc-wkr-1's session" || bad "wrong node: $(tr '\n' ' ' <<<"$out")"
    out="$(edge get "$sid" --node "$a1" --ticket "${ticket}x")"
    grep -qi "permission" <<<"$out" && ok "a tampered ticket is refused (PermissionDenied)" || bad "tampered ticket: $(tr '\n' ' ' <<<"$out")"
    key="edge-smoke-$RANDOM"
    a="$(edge start sessions --task shell --node "$a1" --ticket "$ticket" --idempotency-key "$key")"
    b="$(edge start sessions --task shell --node "$a1" --ticket "$ticket" --idempotency-key "$key")"
    [ -n "$(jq -r '.data.sessionId // empty' <<<"$a" 2>/dev/null)" ] && [ "$(jq -r .data.sessionId <<<"$a")" = "$(jq -r .data.sessionId <<<"$b" 2>/dev/null)" ] &&
        [ "$(jq -r .data.deduplicated <<<"$b")" = true ] && ok "an idempotent retry to the same node is deduplicated" ||
        bad "idempotent edge start: $(tr '\n' ' ' <<<"$a") / $(tr '\n' ' ' <<<"$b")"
    out="$(edge stop "$sid" --node "$a1" --ticket "$ticket")"
    [ "$(jq -r '.data.stopped // empty' <<<"$out" 2>/dev/null)" = true ] && ok "session stop ended it" || bad "session stop: $(tr '\n' ' ' <<<"$out")"
    out="$(edge stop "$sid" --node "$a1" --ticket "$ticket")"
    [ "$(jq -r '.data.stopped | tostring' <<<"$out" 2>/dev/null)" = false ] && ok "a repeated stop answers stopped=false" ||
        bad "repeat stop: $(tr '\n' ' ' <<<"$out")"
    legion instance stop "$(jq -r '.data.sessionId // empty' <<<"$a" 2>/dev/null)" >/dev/null 2>&1
}

scenario_sessions_route() {
    section "sessions-route (a silent node's session answers UNAVAILABLE naming it; PR #36 Phase 3; stops a worker ~2.5 min)"
    legion job run "$jobs_dir/11-sessions.ncl" >/dev/null
    local out id node
    out="$(legion instance create sessions --task shell -j 2>/dev/null)"
    id="$(jq -r '.data.instanceIds[0] // empty' <<<"$out")"
    node="$(jq -r '.data.node // empty' <<<"$out")"
    [ -n "$id" ] || { bad "no session started"; return; }
    sc_ssh "$node" sudo systemctl stop legion-worker
    sleep 150
    out="$(legion instance get "$id" -j 2>&1)"
    if grep -q UNAVAILABLE <<<"$out" && grep -q "$node" <<<"$out"; then
        ok "get on a silent node's session: UNAVAILABLE naming $node"
    else
        bad "get on a silent node's session: $(jq -c .error <<<"$out" 2>/dev/null || tail -1 <<<"$out")"
    fi
    sc_ssh "$node" sudo systemctl start legion-worker
    eventually 180 sh -c "legion instance get $id -j >/dev/null 2>&1" && ok "$id answers again once $node is back" ||
        bad "$id did not come back with $node"
    legion node uncordon "$node" >/dev/null 2>&1
}

scenario_sandbox_pressure() {
    section "sandbox-pressure (memory pressure suspends an onPressure sandbox, evicts a bursting hog)"
    ensure_sbx || { bad "no running $SBX_ID"; return; }
    legion job run "$jobs_dir/12-mem-hog.ncl" >/dev/null || { bad "12-mem-hog.ncl was not accepted"; return; }
    if eventually 300 events_has node.pressure evict-bursting --job mem-hog; then
        ok "the hog was evicted by evict-bursting"
    else
        bad "no evict-bursting node.pressure event for mem-hog in 5 minutes"
    fi
    if events_has node.pressure suspend --instance "$SBX_ID"; then
        ok "$SBX_ID suspended by the pressure response before any eviction (reason $(inst "$SBX_ID" suspendReason))"
    else
        skip "$SBX_ID was not suspended (its node may not have been the one short of memory)"
    fi
    legion job stop mem-hog >/dev/null 2>&1
    curl -fsS --max-time 5 "http://$leader_ip:4749/metrics" 2>/dev/null | has legion_node_pressure_actions_total &&
        ok "legion_node_pressure_actions_total is exported" || bad "no legion_node_pressure_actions_total on the leader"
    if state_is "$SBX_ID" suspended; then
        legion instance start "$SBX_ID" --wait >/dev/null 2>&1
        eventually 120 sbx_ok "$SBX_ID" true && ok "$SBX_ID resumed after the pressure cleared" || bad "$SBX_ID did not resume"
    fi
}

scenario_sandbox_preempt() {
    section "sandbox-preempt (a high-band microVM displaces a low-band one)"
    legion job stop sessions >/dev/null 2>&1
    legion job stop mem-hog >/dev/null 2>&1
    local mib="${SC_FILL_MIB:-3072}" plan
    legion job run "$jobs_dir/13-band.ncl" --var name=band-fill --var band=low --var-code count=6 --var-code "mib=$mib" >/dev/null ||
        { bad "band-fill was not accepted"; return; }
    eventually 300 sh -c "legion job logs band-fill 2>&1 | grep -q 'band vm'" && ok "low-band microVMs booted" || bad "no band-fill VM booted"
    sleep 30
    plan="$(legion job plan "$jobs_dir/13-band.ncl" --var name=band-claim --var band=high --var-code count=1 --var-code "mib=$mib" 2>&1)"
    grep -qi "would preempt" <<<"$plan" && ok "job plan names the victim: $(grep -i -A1 'would preempt' <<<"$plan" | tail -1 | tr -s ' ')" ||
        bad "job plan shows no preemption (size SC_FILL_MIB so the fill leaves less than that free): $(grep -i preempt <<<"$plan" | head -2 | tr '\n' ' ')"
    legion job run "$jobs_dir/13-band.ncl" --var name=band-claim --var band=high --var-code count=1 --var-code "mib=$mib" >/dev/null
    eventually 240 events_has allocation.preempted band-fill && ok "allocation.preempted names band-fill" || bad "no allocation.preempted event"
    eventually 300 sh -c "legion job logs band-claim 2>&1 | grep -q 'band vm'" && ok "the high-band microVM booted in the room" ||
        bad "band-claim never booted"
    legion job stop band-claim >/dev/null 2>&1
    legion job stop band-fill >/dev/null 2>&1
}

scenario_sandbox_destroy() {
    section "sandbox-destroy (volume reclaimed, id never reused)"
    [ -n "$(inst "$SBX_ID" state)" ] || { skip "no $SBX_ID to destroy"; return; }
    legion instance destroy "$SBX_ID" >/dev/null && ok "destroyed $SBX_ID" || bad "destroy $SBX_ID"
    eventually 120 sh -c "! legion volume list --job sandboxes -j 2>/dev/null | grep -q $SBX_ID" && ok "its volume is reclaimed" ||
        bad "the volume of $SBX_ID is still listed"
    # The default list hides tombstones; --all keeps the record of where it went.
    legion volume list --job sandboxes --all -j 2>/dev/null | jq -e --arg id "$SBX_ID" \
        '.data[] | select(.instanceId == $id and .state == "destroyed")' >/dev/null &&
        ok "--all still shows its destroyed tombstone" || bad "no destroyed tombstone for $SBX_ID under --all"
    legion instance create sandboxes --task ws --id "$SBX_ID" >/dev/null 2>&1 && bad "a destroyed id was created again" ||
        ok "a destroyed id cannot be created again"
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

sandbox_group=(sandbox-exec sandbox-net sandbox-suspend sandbox-idle sandbox-store
    sessions sessions-nodes sessions-restart sessions-edge sandbox-pressure sandbox-preempt sandbox-destroy)
scenarios=("$@")
[ ${#scenarios[@]} -gt 0 ] || scenarios=(cluster container exec sandbox kv dag fullstack agents redis "${sandbox_group[@]}" failover)
for s in "${scenarios[@]}"; do
    if [ "$s" = sandboxes ]; then
        for g in "${sandbox_group[@]}"; do "scenario_${g//-/_}"; done
    else
        "scenario_${s//-/_}"
    fi
done
printf '\n\033[1m%d passed, %d failed, %d skipped\033[0m\n' "$pass" "$fail" "$skipped"
[ "$fail" -eq 0 ]
