#!/usr/bin/env bash
# Shared helpers for the campaign scripts. Source this; never run it.
#
#   source "$(dirname "$0")/env.sh"
#
# Requires: jq, aws CLI (with credentials for the rig's account), ssh/scp
# with the rig key loaded. Off-rig it also requires terraform; on-rig it
# does not (see "Where am I running?" below).
set -euo pipefail

AWS_BENCH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$AWS_BENCH_ROOT/../.." && pwd)"

# Make the Rust toolchain reachable however this script was invoked.
# rustup puts cargo in ~/.cargo/bin and appends its env to ~/.bashrc,
# which Ubuntu's .bashrc skips for non-interactive shells — and every
# remote invocation (`ssh host "scripts/build-and-upload.sh"`, a tmux
# command string) is non-interactive. Without this, build-and-upload.sh
# dies on `cargo: command not found` after the campaign has already
# built the rig.
if ! command -v cargo >/dev/null 2>&1 && [ -f "$HOME/.cargo/env" ]; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi
TF_DIR="$AWS_BENCH_ROOT/terraform"
RESULTS_ROOT="$AWS_BENCH_ROOT/results"

# shellcheck source=versions.env
source "$AWS_BENCH_ROOT/scripts/versions.env"

# --- Where am I running? ------------------------------------------------
# A campaign can be driven from two places, and every "reach that box"
# address below depends on which:
#
#   off-rig   your machine. Topology comes from terraform state, and the
#             boxes are reached on their PUBLIC IPs — which is why
#             terraform/security.tf pins admin_cidr to your address.
#   on-rig    the loadgen box itself, via scripts/remote-campaign.sh.
#             There is no terraform state there, so topology comes from
#             the rig manifest, and boxes are reached on PRIVATE IPs.
#
# The private-IP rule on-rig is not a style preference. Same-VPC traffic
# addressed to another instance's *public* IP hairpins out through the
# internet gateway and arrives with the sender's public IP as its source,
# so the security group's intra-rig `self` rule does not match it and the
# connection is refused. Private IPs stay inside the VPC and match.
#
# Detection is the manifest's presence; BENCH_ON_RIG=0|1 forces it.
RIG_MANIFEST="${RIG_MANIFEST:-/opt/bench/rig.json}"
if [ -z "${BENCH_ON_RIG:-}" ]; then
    if [ -f "$RIG_MANIFEST" ]; then BENCH_ON_RIG=1; else BENCH_ON_RIG=0; fi
fi

# Scenario JSONs live in the repo off-rig; install-loadgen.sh stages them
# next to the driver on-rig, where the repo isn't checked out.
if [ "$BENCH_ON_RIG" = 1 ]; then
    SCENARIOS_DIR="${SCENARIOS_DIR:-/opt/bench/scenarios}"
else
    SCENARIOS_DIR="${SCENARIOS_DIR:-$REPO_ROOT/benchmarks/scheduler/scenarios}"
fi

# The rig key on the loadgen box. This is not new with unattended
# campaigns: the drills in experiments/ build fault-injection commands
# that the driver runs ON the loadgen box ("stop the manager on the
# victim"), so that box has always needed to reach the rest of the rig
# over ssh. install-loadgen.sh puts the key here; RSH_FROM_LOADGEN is the
# prefix to use in any command string destined for that box, which has no
# ssh agent to fall back on.
#
# It skips known_hosts on purpose. Spot workers come and go and reuse
# private IPs, so a remembered host key goes stale between campaigns and
# accept-new refuses the hop. On 2026-10-10 that silently cancelled a K8s
# storm drill's kill. These hops stay inside the rig's VPC.
RIG_KEY_ON_LOADGEN=/home/ubuntu/.ssh/bench_key
RSH_FROM_LOADGEN="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -o IdentitiesOnly=yes -i $RIG_KEY_ON_LOADGEN"

# On-rig the operator's ssh agent isn't there either, so use that key.
if [ "$BENCH_ON_RIG" = 1 ]; then
    BENCH_SSH_KEY="${BENCH_SSH_KEY:-$RIG_KEY_ON_LOADGEN}"
fi
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o User=ubuntu)
[ -n "${BENCH_SSH_KEY:-}" ] && SSH_OPTS+=(-o IdentitiesOnly=yes -i "$BENCH_SSH_KEY")

# The local private key matching var.ssh_public_key, for staging onto the
# loadgen box. Deliberately not folded into SSH_OPTS off-rig: your own ssh
# should keep using whatever agent and config it already has.
resolve_local_ssh_key() {
    local k="${BENCH_SSH_KEY:-}" candidate
    if [ -z "$k" ]; then
        for candidate in "$HOME/.ssh/id_ed25519" "$HOME/.ssh/id_rsa"; do
            [ -f "$candidate" ] && k="$candidate" && break
        done
    fi
    [ -n "$k" ] && [ -f "$k" ] || return 1
    echo "$k"
}

tf_output() {
    terraform -chdir="$TF_DIR" output -json "$1"
}

# Populates the rig topology variables, from terraform state (off-rig) or
# the manifest (on-rig). Both paths set the same variables:
#
#   CP_PRIVATE_IPS[] LOADGEN_PRIVATE_IP   baked into configs, always private
#   CP_PUBLIC_IPS[]  LOADGEN_PUBLIC_IP    for URLs a human opens
#   CP_ADDRS[]       LOADGEN_ADDR         "reach that box" — use these for
#                                         ssh/scp/curl, they follow the mode
load_rig() {
    if [ "$BENCH_ON_RIG" = 1 ]; then
        _load_rig_manifest
    else
        _load_rig_terraform
    fi

    if [ "$BENCH_ON_RIG" = 1 ]; then
        CP_ADDRS=("${CP_PRIVATE_IPS[@]}")
        LOADGEN_ADDR="$LOADGEN_PRIVATE_IP"
    else
        CP_ADDRS=("${CP_PUBLIC_IPS[@]}")
        LOADGEN_ADDR="$LOADGEN_PUBLIC_IP"
    fi
    export CP_PUBLIC_IPS CP_PRIVATE_IPS CP_ADDRS LOADGEN_PUBLIC_IP \
        LOADGEN_PRIVATE_IP LOADGEN_ADDR BUCKET REGION WORKER_ASG \
        CONTROL_PLANE_TYPE LOADGEN_TYPE WORKER_ON_DEMAND_PCT ARCHIVE_BUCKET
}

_load_rig_terraform() {
    CP_PUBLIC_IPS=($(tf_output control_plane_public_ips | jq -r '.[]'))
    CP_PRIVATE_IPS=($(tf_output control_plane_private_ips | jq -r '.[]'))
    LOADGEN_PUBLIC_IP=$(tf_output loadgen_public_ip | jq -r .)
    LOADGEN_PRIVATE_IP=$(tf_output loadgen_private_ip | jq -r .)
    BUCKET=$(tf_output bucket | jq -r .)
    REGION=$(tf_output region | jq -r .)
    WORKER_ASG=$(tf_output worker_asg_name | jq -r .)
    CONTROL_PLANE_TYPE=$(tf_output control_plane_type | jq -r .)
    LOADGEN_TYPE=$(tf_output loadgen_type | jq -r .)
    WORKER_ON_DEMAND_PCT=$(tf_output worker_on_demand_percentage | jq -r .)
    ARCHIVE_BUCKET=$(tf_output archive_bucket | jq -r .)
}

_load_rig_manifest() {
    local m="$RIG_MANIFEST"
    [ -f "$m" ] || {
        echo "no rig manifest at $m — this box was not staged by remote-campaign.sh" >&2
        return 1
    }
    CP_PUBLIC_IPS=($(jq -r '.control_plane_public_ips[]' "$m"))
    CP_PRIVATE_IPS=($(jq -r '.control_plane_private_ips[]' "$m"))
    LOADGEN_PUBLIC_IP=$(jq -r '.loadgen_public_ip' "$m")
    LOADGEN_PRIVATE_IP=$(jq -r '.loadgen_private_ip' "$m")
    BUCKET=$(jq -r '.bucket' "$m")
    REGION=$(jq -r '.region' "$m")
    WORKER_ASG=$(jq -r '.worker_asg_name' "$m")
    CONTROL_PLANE_TYPE=$(jq -r '.control_plane_type' "$m")
    LOADGEN_TYPE=$(jq -r '.loadgen_type' "$m")
    WORKER_ON_DEMAND_PCT=$(jq -r '.worker_on_demand_percentage // 0' "$m")
    ARCHIVE_BUCKET=$(jq -r '.archive_bucket // ""' "$m")
}

# Serialize the topology for the loadgen box. Everything the scripts read
# from terraform lives here, so the on-rig path needs no state and no
# terraform binary.
write_rig_manifest() { # write_rig_manifest > rig.json
    jq -n \
        --argjson cp_pub "$(printf '%s\n' "${CP_PUBLIC_IPS[@]}" | jq -R . | jq -s .)" \
        --argjson cp_priv "$(printf '%s\n' "${CP_PRIVATE_IPS[@]}" | jq -R . | jq -s .)" \
        --arg lg_pub "$LOADGEN_PUBLIC_IP" --arg lg_priv "$LOADGEN_PRIVATE_IP" \
        --arg bucket "$BUCKET" --arg region "$REGION" --arg asg "$WORKER_ASG" \
        --arg cp_type "$CONTROL_PLANE_TYPE" --arg lg_type "$LOADGEN_TYPE" \
        --argjson on_demand_pct "${WORKER_ON_DEMAND_PCT:-0}" \
        --arg archive_bucket "${ARCHIVE_BUCKET:-}" \
        --arg written_at "$(date -u +%FT%TZ)" \
        '{control_plane_public_ips: $cp_pub, control_plane_private_ips: $cp_priv,
          loadgen_public_ip: $lg_pub, loadgen_private_ip: $lg_priv,
          bucket: $bucket, region: $region, worker_asg_name: $asg,
          control_plane_type: $cp_type, loadgen_type: $lg_type,
          worker_on_demand_percentage: $on_demand_pct,
          archive_bucket: $archive_bucket,
          written_at: $written_at}'
}

# Current spot-fleet members: "instance-id public-ip private-ip" per line.
# Use worker_addr to pick the reachable one for the current mode.
worker_instances() {
    local ids
    ids=$(aws autoscaling describe-auto-scaling-groups \
        --region "$REGION" \
        --auto-scaling-group-names "$WORKER_ASG" \
        --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId' \
        --output text)
    [ -n "$ids" ] || return 0
    # shellcheck disable=SC2086
    # A just-InService instance can briefly describe with no private IP
    # (the 10k K8s rerun on 2026-10-10 died ssh-ing to "None"). Leave it
    # out until it has one, so wait_for_workers keeps waiting for it.
    aws ec2 describe-instances --region "$REGION" --instance-ids $ids \
        --query 'Reservations[].Instances[].[InstanceId,PublicIpAddress,PrivateIpAddress]' \
        --output text | awk '$3 != "None"'
}

# worker_addr <public-ip> <private-ip> -> the one this mode can reach.
worker_addr() {
    if [ "$BENCH_ON_RIG" = 1 ]; then echo "$2"; else echo "$1"; fi
}

# Resize the spot fleet. Deliberately the ASG API and not `terraform
# apply -var worker_instances=N`: desired capacity is a per-step runtime
# knob, the loadgen box has no terraform state, and giving it the IAM to
# mutate the whole rig to turn one dial is a bad trade. The ASG ignores
# desired_capacity drift (terraform/instances.tf) so the two never fight.
scale_workers() { # scale_workers <instance-count>
    local want="$1"
    log "scaling spot fleet to $want instance(s)"
    aws autoscaling set-desired-capacity --region "$REGION" \
        --auto-scaling-group-name "$WORKER_ASG" \
        --desired-capacity "$want" --no-honor-cooldown
}

# Block until the fleet has at least <count> InService instances.
wait_for_workers() { # wait_for_workers <count> [timeout-secs]
    local want="$1" timeout="${2:-600}" start have
    start=$(date +%s)
    while :; do
        have=$(worker_instances | grep -c . || true)
        [ "$have" -ge "$want" ] && return 0
        if (($(date +%s) - start > timeout)); then
            echo "spot fleet reached $have/$want instances in ${timeout}s" >&2
            return 1
        fi
        sleep 15
    done
}

# --- The sim fleet's identity, and how a cell proves it ----------------
#
# start-sim-fleet.sh names slice `i` with `--name-prefix sim-<i>`, and
# src/worker/simulate.rs appends `-<profile>-<n>`, so every node this rig
# creates is `sim-<slice>-<profile>-<n>`. Those names are REUSED run to
# run, deliberately: Legion has no node-deregistration RPC
# (`Store::remove_node` exists but has nothing calling it in production),
# so a stopped fleet leaves every row behind. Name reuse is the only
# thing bounding the node table — a unique per-run prefix would make
# every run leak its whole fleet instead of overwriting the last one's.
#
# The consequence a cell has to defend against: the node table is a
# high-water mark of every fleet the control plane has ever seen. A
# 4000-node experiment followed by a 1000-node cell leaves 3000 orphan
# rows, all of them `lost`, all of them still counted by
# `SELECT COUNT(*) FROM node`. That is why the gates below assert an
# EXACT census instead of a lower bound.
SIM_NAME_PREFIX="${SIM_NAME_PREFIX:-sim}"

# What start-sim-fleet.sh last started, written on the loadgen box so any
# later script (run-step.sh, run-campaign.sh) can ask what is actually
# running rather than assume.
FLEET_STATE_REMOTE="${FLEET_STATE_REMOTE:-/opt/bench/runs/fleet-state.json}"

# One SQL census of the manager's node table -> a JSON object:
#   {"total":N,"active":N,"lost":N,"other":N,"unavailable":N,"foreign_ids":N}
#
# `foreign_ids` counts rows whose name is NOT from one of this fleet's
# slices: leftovers from a wider fleet (`sim-4-…` when this run has three
# instances), from another scenario (the profile name is part of the
# node name), or a real worker that wandered in. Health comes from
# `node_health` — the `node.status` column was moved there by the
# node/node_health split, and an absent row means `active`, which is the
# same COALESCE every liveness query in src/storage/store.rs uses.
#
# `unavailable` is counted separately because it is a different failure
# with the same symptom: `node.availability` is the column the SCHEDULER
# gates placement on, node loss sets it to `unavailable` on the control
# lane, and recovery has to undo both halves. A node that came back
# `active` but stayed `unavailable` reads as healthy everywhere and never
# receives work — src/storage/store.rs calls that out as presenting like
# a scheduler bug and being almost impossible to diagnose from outside.
node_census() { # node_census <manager-addr> <slice-count>
    local manager="$1" slices="$2" i like=""
    for ((i = 0; i < slices; i++)); do
        [ -n "$like" ] && like="$like OR "
        like="${like}node.id LIKE '$SIM_NAME_PREFIX-$i-%'"
    done
    # No slices means no name can be legitimate, so every row is foreign.
    [ -n "$like" ] || like="0=1"
    local sql="SELECT COUNT(*) AS total,
        COALESCE(SUM(CASE WHEN COALESCE(h.status,'active')='active' THEN 1 ELSE 0 END),0) AS active,
        COALESCE(SUM(CASE WHEN COALESCE(h.status,'active')='lost' THEN 1 ELSE 0 END),0) AS lost,
        COALESCE(SUM(CASE WHEN COALESCE(h.status,'active') NOT IN ('active','lost') THEN 1 ELSE 0 END),0) AS other,
        COALESCE(SUM(CASE WHEN node.availability <> 'available' THEN 1 ELSE 0 END),0) AS unavailable,
        COALESCE(SUM(CASE WHEN ($like) THEN 0 ELSE 1 END),0) AS foreign_ids
        FROM node LEFT JOIN node_health h ON h.node_id = node.id"
    # `{}` for every failure mode — unreachable manager, an error
    # envelope, jq given nothing to read. The caller reads a missing
    # `.total` as "could not census" and keeps waiting, which is the only
    # safe reading: an unanswered query is not evidence of a good fleet.
    local out
    out=$(rsh "$LOADGEN_ADDR" "LEGION_ADDR=$manager legion state query \"$sql\" -j" 2>/dev/null |
        jq -c '.data.rows[0] // {}' 2>/dev/null || true)
    [ -n "$out" ] || out='{}'
    printf '%s\n' "$out"
}

census_line() { # census_line <census-json> <expected>
    jq -r --arg want "$2" \
        '"total=\(.total // "?") active=\(.active // "?") lost=\(.lost // "?") other=\(.other // "?") unavailable=\(.unavailable // "?") foreign=\(.foreign_ids // "?") expected=\($want)"' \
        <<<"$1" 2>/dev/null || echo "census unreadable: $1"
}

# Assert the fleet is EXACTLY what this cell asked for, or fail.
#
#   assert_fleet <manager> <expected-nodes> <slice-count> [timeout-secs]
#
# This replaced `registered -ge NODES`, which passed on two populations
# that make a cell worthless and say nothing in the log:
#   * 4000 rows when the cell wanted 1000 — a previous, wider experiment's
#     orphans, so the cell measured a scheduler carrying four times the
#     nodes it reports;
#   * a fleet where every node is `lost` — COUNT(*) does not look at
#     health, so a completely dead fleet satisfied a >= gate.
# Both are silent failures, which is why the assertion is exact and why
# the observed census is printed on failure.
#
# Retries, because registration is genuinely progressive: rows appear as
# the sim processes work through `--register-rate`, and a node that just
# re-registered can sit `lost` for a tick until its first heartbeat lands.
# It does NOT retry when total already EXCEEDS expected: nothing removes
# a node row, so that number only ever grows and waiting is pure cost.
assert_fleet() { # assert_fleet <manager> <expected> <slices> [timeout]
    local manager="$1" want="$2" slices="$3" timeout="${4:-600}"
    local deadline census total
    deadline=$(($(date +%s) + timeout))
    while :; do
        census=$(node_census "$manager" "$slices")
        total=$(jq -r '.total // -1' <<<"$census" 2>/dev/null || echo -1)
        if [ "$total" = "-1" ]; then
            log "  census unavailable (manager not answering state queries yet)"
        else
            log "  fleet census: $(census_line "$census" "$want")"
            if jq -e --argjson want "$want" \
                '.total == $want and .active == $want and .unavailable == 0 and .foreign_ids == 0' \
                >/dev/null <<<"$census" 2>/dev/null; then
                return 0
            fi
            if [ "$total" -gt "$want" ] 2>/dev/null; then
                echo "fleet is LARGER than this cell asked for: $(census_line "$census" "$want")" >&2
                echo "  Legion has no node-deregistration RPC, so those rows never leave on" >&2
                echo "  their own and no amount of waiting will fix this. Reset the control" >&2
                echo "  plane before the campaign: scripts/reset-control-plane.sh --system <s>" >&2
                return 1
            fi
        fi
        (($(date +%s) < deadline)) || {
            echo "fleet never reached an exact census in ${timeout}s: $(census_line "$census" "$want")" >&2
            echo "  Wanted $want rows, every one of them active AND available, every" >&2
            echo "  name from $SIM_NAME_PREFIX-<0..$((slices - 1))>-*." >&2
            return 1
        }
        sleep 10
    done
}

# Copy the driver, translator, and scenarios onto the loadgen box. Called
# at every campaign start, not only at bringup: the loadgen box otherwise
# keeps whatever was staged when the rig came up, and the first Nomad
# smoke of 2026-10-09 ran a driver and translator from before that day's
# fixes — the old `mock` driver name, no HTTP submitter — and placed
# nothing while the repo on the operator box was already correct.
stage_driver() {
    local stage
    stage=$(mktemp -d)
    cp -r "$AWS_BENCH_ROOT/driver" "$AWS_BENCH_ROOT/workloads" "$stage/"
    mkdir -p "$stage/scenarios"
    cp "$SCENARIOS_DIR"/*.json "$stage/scenarios/"
    find "$stage" -name __pycache__ -prune -exec rm -rf {} +
    tar -C "$stage" -czf - . | rsh "$LOADGEN_ADDR" "tar -C /opt/bench -xzf -"
    rm -rf "$stage"
}

# Ready-node count for the competitor systems, which have no node_census.
ready_node_count() { # ready_node_count <nomad|k8s>
    case "$1" in
    nomad)
        rsh "$LOADGEN_ADDR" "curl -fsS -m 20 http://${CP_PRIVATE_IPS[0]}:4646/v1/nodes" 2>/dev/null |
            jq '[.[] | select(.Status == "ready" and .SchedulingEligibility == "eligible")] | length' 2>/dev/null
        ;;
    k8s)
        rsh "$LOADGEN_ADDR" "kubectl get nodes -l type=kwok -o json" 2>/dev/null |
            jq '[.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length' 2>/dev/null
        ;;
    esac
}

# The competitor systems' equivalent of assert_fleet: exactly <want> ready
# nodes, refusing a larger count rather than measuring a dirty cluster.
wait_for_ready_nodes() { # wait_for_ready_nodes <nomad|k8s> <want> [timeout]
    local system="$1" want="$2" timeout="${3:-600}" deadline ready
    deadline=$(($(date +%s) + timeout))
    while :; do
        ready=$(ready_node_count "$system")
        log "  $system ready nodes: ${ready:-?}/$want"
        [ "${ready:-}" = "$want" ] && return 0
        if [ -n "$ready" ] && [ "$ready" -gt "$want" ] 2>/dev/null; then
            echo "$system has $ready ready nodes, more than the $want this cell asked for — reset the control plane" >&2
            return 1
        fi
        (($(date +%s) < deadline)) || {
            echo "$system never reached $want ready nodes in ${timeout}s (last: ${ready:-unknown})" >&2
            return 1
        }
        sleep 10
    done
}

# What the running sim fleet is, as JSON, or {} when nothing recorded it.
fleet_state() {
    rsh "$LOADGEN_ADDR" "cat '$FLEET_STATE_REMOTE' 2>/dev/null" 2>/dev/null |
        jq -c . 2>/dev/null || echo '{}'
}

write_fleet_state() { # write_fleet_state <system> <scenario> <nodes> <slices> <manager>
    local json
    json=$(jq -n --arg system "$1" --arg scenario "$2" --argjson nodes "$3" \
        --argjson slices "$4" --arg manager "$5" --arg started_at "$(date -u +%FT%TZ)" \
        '{system: $system, scenario: $scenario, nodes: $nodes, slices: $slices,
          manager: $manager, started_at: $started_at}')
    rsh "$LOADGEN_ADDR" "mkdir -p \$(dirname '$FLEET_STATE_REMOTE') &&
        cat >'$FLEET_STATE_REMOTE'" <<<"$json"
}

clear_fleet_state() {
    rsh "$LOADGEN_ADDR" "rm -f '$FLEET_STATE_REMOTE'" 2>/dev/null || true
}

# Backlog of scheduler work the manager has not started yet: pending +
# in-flight evals. This is the number a cell has to start from zero-ish,
# because a fleet stop/start marks thousands of nodes lost and each loss
# raises a reschedule eval — measured at 5760 reschedules against 2503
# submits, i.e. the cell's job submissions were a minority of the work
# the eval workers were doing, and the "throughput" it reported was the
# scheduler splitting its attention with the churn we caused.
eval_backlog() { # eval_backlog <manager-addr>
    rsh "$LOADGEN_ADDR" "LEGION_ADDR=$1 legion state query \
        \"SELECT COUNT(*) AS n FROM eval WHERE status IN ('pending','in_flight')\" -j" 2>/dev/null |
        jq -r '.data.rows[0].n // -1' 2>/dev/null || echo -1
}

# Wait for that backlog to drain before a cell starts measuring.
#
#   wait_for_eval_drain <manager> [floor] [timeout-secs]
#
# Echoes the backlog it settled at on stdout (so the caller can archive
# it) and never fails the cell: a contaminated cell that says so in its
# archive is worth more than a cell that refused to run.
wait_for_eval_drain() { # -> settled depth on stdout
    local manager="$1" floor="${2:-10}" timeout="${3:-600}"
    local deadline depth quiet=0
    deadline=$(($(date +%s) + timeout))
    while :; do
        depth=$(eval_backlog "$manager")
        if [ "$depth" = "-1" ]; then
            log "  eval backlog unreadable — continuing without a drain wait"
            echo -1
            return 0
        fi
        if [ "$depth" -le "$floor" ]; then
            quiet=$((quiet + 1))
            # Two consecutive quiet polls, because the queue dips between
            # a loss storm's batches and one sample lands in the dip.
            if [ "$quiet" -ge 2 ]; then
                log "  eval backlog drained to $depth"
                echo "$depth"
                return 0
            fi
        else
            quiet=0
            log "  eval backlog $depth (waiting for <= $floor)"
        fi
        (($(date +%s) < deadline)) || {
            log "  WARNING: eval backlog still $depth after ${timeout}s — this cell competes"
            log "           with that churn; the depth is recorded in its archive"
            echo "$depth"
            return 0
        }
        sleep 5
    done
}

# ACLs are on by default (docs/auth-and-acls.md). The manager that first
# leads the control lane mints the initial management token into
# <dataDir>/bootstrap-token; the rig's managers run in /var/lib/legion, so
# that is /var/lib/legion/.legion/bootstrap-token. Copy it to the ssh user's
# ~/.legion/token on every control-plane box and on the loadgen, where the
# CLI and legion-loadgen read it, and export it for local calls.
rig_install_cli_token() { # rig_install_cli_token
    local token="" ip
    for ip in "${CP_ADDRS[@]}"; do
        token=$(rsh "$ip" "sudo cat /var/lib/legion/.legion/bootstrap-token 2>/dev/null" |
            tr -d '[:space:]') || true
        [ -n "$token" ] && break
    done
    [ -n "$token" ] || {
        echo "no manager wrote /var/lib/legion/.legion/bootstrap-token" >&2
        return 1
    }
    for ip in "${CP_ADDRS[@]}" "$LOADGEN_ADDR"; do
        printf '%s\n' "$token" |
            rsh "$ip" "umask 077 && mkdir -p ~/.legion && cat >~/.legion/token"
    done
    LEGION_TOKEN="$token"
    export LEGION_TOKEN
}

rsh() { # rsh <ip> <command...>
    local ip="$1"
    shift
    ssh "${SSH_OPTS[@]}" "$ip" "$@"
}

rcp() { # rcp <local-path> <ip>:<remote-path>
    scp -q "${SSH_OPTS[@]}" "$1" "$2"
}

# Run a local script remotely as root, with arguments.
rsudo_script() { # rsudo_script <ip> <script-path> [args...]
    local ip="$1" script="$2"
    shift 2
    # shellcheck disable=SC2029
    ssh "${SSH_OPTS[@]}" "$ip" "sudo bash -s -- $*" <"$script"
}

wait_for_ssh() { # wait_for_ssh <ip> [timeout-secs]
    local ip="$1" timeout="${2:-300}" start
    start=$(date +%s)
    until rsh "$ip" true 2>/dev/null; do
        if (($(date +%s) - start > timeout)); then
            echo "timed out waiting for ssh on $ip" >&2
            return 1
        fi
        sleep 5
    done
}

# Wait for an HTTP endpoint that lives ON a rig box, from wherever this
# script happens to be running.
#
# This exists because `wait_for_http` below probes from the local machine,
# and off-rig that can only reach what security.tf opens to admin_cidr:
# SSH, Grafana (3000), and Prometheus (9090). Everything else the rig
# serves — the manager's health/metrics port 4749, node_exporter's 9100,
# Nomad's 4646 — is intra-rig only, so polling it from an operator box
# does not fail because the service is unhealthy. It fails because the
# packet never arrives, and the timeout then reports a perfectly healthy
# manager as dead. Run the probe on the box itself instead.
#
# On-rig, poll directly: the private IP matches the security group's
# `self` rule, and there is no ssh hop worth paying for.
# Single non-blocking probe, same addressing rule as rig_http_ready.
# For callers polling several boxes to find one that answers.
rig_probe_once() { # rig_probe_once <ip> <port/path>
    if [ "$BENCH_ON_RIG" = 1 ]; then
        curl -fsS -m 3 "http://$1:$2" >/dev/null 2>&1
    else
        rsh "$1" "curl -fsS -m 3 'http://localhost:$2' >/dev/null 2>&1"
    fi
}

rig_http_ready() { # rig_http_ready <ip> <port/path> [timeout-secs]
    local ip="$1" suffix="$2" timeout="${3:-180}"
    if [ "$BENCH_ON_RIG" = 1 ]; then
        wait_for_http "http://$ip:$suffix" "$timeout"
        return
    fi
    rsh "$ip" "for _ in \$(seq 1 $((timeout / 5))); do
                   curl -fsS -m 3 'http://localhost:$suffix' >/dev/null 2>&1 && exit 0
                   sleep 5
               done
               exit 1" || {
        echo "timed out waiting for http://localhost:$suffix on $ip" >&2
        return 1
    }
}

wait_for_http() { # wait_for_http <url> [timeout-secs]  (runs locally)
    local url="$1" timeout="${2:-180}" start
    start=$(date +%s)
    until curl -fsS -m 3 "$url" >/dev/null 2>&1; do
        if (($(date +%s) - start > timeout)); then
            echo "timed out waiting for $url" >&2
            return 1
        fi
        sleep 3
    done
}

log() {
    echo "[$(date -u +%H:%M:%S)] $*" >&2
}

# envsubst without envsubst (gettext isn't a given on operator machines):
# substitutes ONLY ${VAR} for the names passed as arguments, from the
# environment, leaving every other ${...} untouched. The script rides in
# -c because stdin carries the template.
#   FOO=x render_tpl FOO <in.tpl >out
render_tpl() {
    python3 -c '
import os, re, sys
allowed = set(sys.argv[1:])
def sub(match):
    name = match.group(1)
    return os.environ.get(name, match.group(0)) if name in allowed else match.group(0)
sys.stdout.write(re.sub(r"\$\{(\w+)\}", sub, sys.stdin.read()))
' "$@"
}
