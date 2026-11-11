#!/usr/bin/env bash
# Shared settings for the sandbox-boot rig. Source this file, don't run it.
#
# The rig measures how fast Legion starts sandbox sessions on real KVM: small
# spot boxes with nested virtualization run cloud-hypervisor microVMs, one or
# three managers route starts, and a load box drives them. Everything it
# creates is tagged Project=legion-sbx-bench, which is what down.sh deletes
# by. Nothing here touches the spot-check or benchmark rigs.

export AWS_PROFILE="${AWS_PROFILE:-legion-bench}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_PAGER=""

SB_PROJECT="legion-sbx-bench"
SB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SB_REPO="$(cd "$SB_DIR/../../.." && pwd)"
SB_STATE="${SB_STATE:-$SB_DIR/.state}"
SB_KEY="$SB_STATE/id_ed25519"

# One AZ keeps cross-node traffic free, and us-east-1a had the cheapest c8i
# spot capacity when the rig was written.
SB_AZ="${SB_AZ:-us-east-1a}"

# c8i is the family with nested virtualization, so every box can run KVM
# guests without paying for bare metal:
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-ec2-nested-virtualization.html
SB_MANAGER_TYPE="${SB_MANAGER_TYPE:-c8i.large}"
SB_WORKER_TYPE="${SB_WORKER_TYPE:-c8i.xlarge}"
SB_LOAD_TYPE="${SB_LOAD_TYPE:-c8i.xlarge}"
SB_BUILD_TYPE="${SB_BUILD_TYPE:-c8i.8xlarge}"

SB_STATE="${SB_STATE:-$SB_DIR/.state}"
# Box names, overridable per run (SB_WORKERS="sbx-wkr-1 sbx-wkr-2") or for the
# rig's current shape in .state/topology (the same three variables).
# shellcheck disable=SC1091
[ -f "$SB_STATE/topology" ] && source "$SB_STATE/topology"
read -r -a SB_MANAGERS <<<"${SB_MANAGERS:-sbx-mgr-1}"
read -r -a SB_WORKERS <<<"${SB_WORKERS:-sbx-wkr-1}"
read -r -a SB_LOADERS <<<"${SB_LOADERS:-}"

SB_CH_VERSION="${SB_CH_VERSION:-v51.2}"
SB_KERNEL_RELEASE="ch-release-v6.16.9-20261001"

SB_ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
SB_BUCKET="${SB_PROJECT}-${SB_ACCOUNT}"

mkdir -p "$SB_STATE"

sb_log() { printf '\033[1m==> %s\033[0m\n' "$*" >&2; }

# "name public_ip private_ip instance_id type" for every running rig box.
sb_hosts() {
    aws ec2 describe-instances \
        --filters "Name=tag:Project,Values=$SB_PROJECT" "Name=instance-state-name,Values=pending,running" \
        --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,PublicIpAddress,PrivateIpAddress,InstanceId,InstanceType]' \
        --output text | sort
}

sb_public_ip() { sb_hosts | awk -v n="$1" '$1 == n { print $2 }'; }
sb_private_ip() { sb_hosts | awk -v n="$1" '$1 == n { print $3 }'; }

sb_ssh() {
    local name="$1"
    shift
    ssh -i "$SB_KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$SB_STATE/known_hosts" \
        -o ConnectTimeout=10 -o LogLevel=ERROR -o ServerAliveInterval=30 "ubuntu@$(sb_public_ip "$name")" "$@"
}

sb_scp_to() { # name local remote
    scp -q -i "$SB_KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$SB_STATE/known_hosts" \
        -o LogLevel=ERROR "$2" "ubuntu@$(sb_public_ip "$1"):$3"
}
