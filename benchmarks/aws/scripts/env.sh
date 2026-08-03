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
RIG_KEY_ON_LOADGEN=/home/ubuntu/.ssh/bench_key
RSH_FROM_LOADGEN="ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o IdentitiesOnly=yes -i $RIG_KEY_ON_LOADGEN"

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
    aws ec2 describe-instances --region "$REGION" --instance-ids $ids \
        --query 'Reservations[].Instances[].[InstanceId,PublicIpAddress,PrivateIpAddress]' \
        --output text
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
