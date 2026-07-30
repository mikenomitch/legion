#!/usr/bin/env bash
# Shared helpers for the campaign scripts. Source this; never run it.
#
#   source "$(dirname "$0")/env.sh"
#
# Requires: terraform, jq, aws CLI (with credentials for the rig's
# account), ssh/scp with the key from terraform.tfvars loaded.
set -euo pipefail

AWS_BENCH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$AWS_BENCH_ROOT/../.." && pwd)"
TF_DIR="$AWS_BENCH_ROOT/terraform"
RESULTS_ROOT="$AWS_BENCH_ROOT/results"

# shellcheck source=versions.env
source "$AWS_BENCH_ROOT/scripts/versions.env"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o User=ubuntu)

tf_output() {
    terraform -chdir="$TF_DIR" output -json "$1"
}

# Populates the rig topology variables from terraform state.
load_rig() {
    CP_PUBLIC_IPS=($(tf_output control_plane_public_ips | jq -r '.[]'))
    CP_PRIVATE_IPS=($(tf_output control_plane_private_ips | jq -r '.[]'))
    LOADGEN_PUBLIC_IP=$(tf_output loadgen_public_ip | jq -r .)
    LOADGEN_PRIVATE_IP=$(tf_output loadgen_private_ip | jq -r .)
    BUCKET=$(tf_output bucket | jq -r .)
    REGION=$(tf_output region | jq -r .)
    WORKER_ASG=$(tf_output worker_asg_name | jq -r .)
    export CP_PUBLIC_IPS CP_PRIVATE_IPS LOADGEN_PUBLIC_IP LOADGEN_PRIVATE_IP \
        BUCKET REGION WORKER_ASG
}

# Current spot-fleet members: "instance-id public-ip private-ip" per line.
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
