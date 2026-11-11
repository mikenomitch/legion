#!/usr/bin/env bash
# Shared settings for the spot-check rig. Source this file, don't run it.
#
# Everything the rig creates is tagged Project=legion-spot-check, which is
# what down.sh deletes by. Nothing here touches the benchmark rig.

export AWS_PROFILE="${AWS_PROFILE:-legion-bench}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_PAGER=""

SC_PROJECT="legion-spot-check"
SC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SC_REPO="$(cd "$SC_DIR/../.." && pwd)"
SC_STATE="$SC_DIR/.state"
SC_KEY="$SC_STATE/id_ed25519"

# One AZ keeps cross-node traffic free and simple.
SC_AZ="${SC_AZ:-us-east-1a}"

# Managers: the cheapest burstable box that comfortably runs a raft member.
SC_MANAGER_TYPE="${SC_MANAGER_TYPE:-t3.small}"
# Workers: spot, and from a family that supports nested virtualization, so
# microVM sandboxes get /dev/kvm without paying for bare metal.
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-ec2-nested-virtualization.html
SC_WORKER_TYPE="${SC_WORKER_TYPE:-m7i.large}"

SC_MANAGERS=(sc-mgr-1 sc-mgr-2 sc-mgr-3)
SC_WORKERS=(sc-wkr-1 sc-wkr-2)

# Pinned versions of what the workers install.
# runc runs the containers; the default user namespace needs 1.2 or newer.
SC_RUNC_VERSION="v1.5.2"
SC_CH_VERSION="v51.2"
SC_KERNEL_RELEASE="ch-release-v6.16.9-20261001"

SC_ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
SC_BUCKET="${SC_PROJECT}-${SC_ACCOUNT}"
SC_ECR="${SC_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"

mkdir -p "$SC_STATE"

# Use this checkout's own CLI build when there is one, so the laptop CLI always
# matches the cluster's version.
for build in "$SC_REPO/target/release" "$SC_REPO/target/debug"; do
    if [ -x "$build/legion" ]; then
        export PATH="$build:$PATH"
        break
    fi
done

sc_log() { printf '\033[1m==> %s\033[0m\n' "$*" >&2; }

# Prints "name public_ip private_ip instance_id" for every running rig box.
sc_hosts() {
    aws ec2 describe-instances \
        --filters "Name=tag:Project,Values=$SC_PROJECT" "Name=instance-state-name,Values=pending,running" \
        --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,PublicIpAddress,PrivateIpAddress,InstanceId]' \
        --output text | sort
}

sc_public_ip() { sc_hosts | awk -v n="$1" '$1 == n { print $2 }'; }
sc_private_ip() { sc_hosts | awk -v n="$1" '$1 == n { print $3 }'; }

sc_ssh() {
    local name="$1"
    shift
    ssh -i "$SC_KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$SC_STATE/known_hosts" \
        -o ConnectTimeout=10 -o LogLevel=ERROR "ubuntu@$(sc_public_ip "$name")" "$@"
}

sc_scp() {
    scp -i "$SC_KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$SC_STATE/known_hosts" \
        -o LogLevel=ERROR "$@"
}
