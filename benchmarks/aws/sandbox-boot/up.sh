#!/usr/bin/env bash
# Creates (or tops up) the sandbox-boot rig: a key pair, one security group,
# an S3 bucket for binaries, and spot boxes for every name in SB_MANAGERS,
# SB_WORKERS, SB_LOADERS, plus the build box with `--build`.
#
#   benchmarks/aws/sandbox-boot/up.sh                 # managers + workers + loaders
#   benchmarks/aws/sandbox-boot/up.sh --build         # also the build box
#   SB_WORKERS="sbx-wkr-1 sbx-wkr-2" benchmarks/aws/sandbox-boot/up.sh
#
# Idempotent: re-running skips boxes that already run. Tear down with down.sh.
set -euo pipefail
source "$(dirname "$0")/env.sh"

with_build=""
[ "${1:-}" = "--build" ] && with_build=1

admin_cidr="${SB_ADMIN_CIDR:-$(curl -fsS https://checkip.amazonaws.com)/32}"
vpc="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)"
subnet="$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$vpc" "Name=availability-zone,Values=$SB_AZ" \
    "Name=default-for-az,Values=true" --query 'Subnets[0].SubnetId' --output text)"
tags="ResourceType=%s,Tags=[{Key=Project,Value=$SB_PROJECT},{Key=Name,Value=%s}]"

if [ ! -f "$SB_KEY" ]; then
    ssh-keygen -q -t ed25519 -N "" -C "$SB_PROJECT" -f "$SB_KEY"
fi
if ! aws ec2 describe-key-pairs --key-names "$SB_PROJECT" >/dev/null 2>&1; then
    sb_log "Importing key pair"
    aws ec2 import-key-pair --key-name "$SB_PROJECT" --public-key-material "fileb://$SB_KEY.pub" \
        --tag-specifications "$(printf "$tags" key-pair "$SB_PROJECT")" >/dev/null
fi

# One group for every box: SSH from the operator, everything inside the group.
# The managers' API stays private; the operator reaches it over SSH.
sg="$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$vpc" "Name=group-name,Values=$SB_PROJECT" \
    --query 'SecurityGroups[0].GroupId' --output text)"
if [ "$sg" = "None" ]; then
    sb_log "Creating security group $SB_PROJECT"
    sg="$(aws ec2 create-security-group --vpc-id "$vpc" --group-name "$SB_PROJECT" \
        --description "legion sandbox-boot rig" \
        --tag-specifications "$(printf "$tags" security-group "$SB_PROJECT")" --query GroupId --output text)"
fi
aws ec2 authorize-security-group-ingress --group-id "$sg" \
    --ip-permissions "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$admin_cidr}]" >/dev/null 2>&1 || true
aws ec2 authorize-security-group-ingress --group-id "$sg" \
    --ip-permissions "IpProtocol=-1,UserIdGroupPairs=[{GroupId=$sg}]" >/dev/null 2>&1 || true

if ! aws s3api head-bucket --bucket "$SB_BUCKET" >/dev/null 2>&1; then
    sb_log "Creating bucket $SB_BUCKET"
    aws s3api create-bucket --bucket "$SB_BUCKET" >/dev/null
    aws s3api put-bucket-tagging --bucket "$SB_BUCKET" --tagging "TagSet=[{Key=Project,Value=$SB_PROJECT}]"
fi

ami="$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
    --query Parameter.Value --output text)"
sed -e "s/@@CH_VERSION@@/$SB_CH_VERSION/g" -e "s/@@KERNEL_RELEASE@@/$SB_KERNEL_RELEASE/g" \
    "$SB_DIR/cloud-init/node.sh" >"$SB_STATE/node-user-data.sh"
existing="$(sb_hosts | awk '{ print $1 }')"

launch() { # name type disk_gb user_data [extra market options]
    local name="$1" type="$2" disk="$3" data="$4" market="${5:-one-time}"
    grep -qx "$name" <<<"$existing" && return 0
    local options="MarketType=spot,SpotOptions={SpotInstanceType=one-time,InstanceInterruptionBehavior=terminate}"
    market_args=(--instance-market-options "$options")
    if [ "$market" = "on-demand" ]; then
        # Managers hold the cluster's raft state, which a spot reclaim takes
        # with it; workers and load boxes are disposable.
        market_args=()
    elif [ "$market" = "persistent" ]; then
        # A persistent request with `stop` lets the build box be stopped
        # between builds and keep its cargo cache on EBS.
        options="MarketType=spot,SpotOptions={SpotInstanceType=persistent,InstanceInterruptionBehavior=stop}"
        market_args=(--instance-market-options "$options")
    fi
    # Bare metal has KVM natively; every other c8i box gets it nested.
    cpu_args=(--cpu-options 'NestedVirtualization=enabled')
    case "$type" in *.metal*) cpu_args=() ;; esac
    sb_log "Launching $name ($type, ${market})"
    aws ec2 run-instances --image-id "$ami" --instance-type "$type" --key-name "$SB_PROJECT" \
        --subnet-id "$subnet" --security-group-ids "$sg" --associate-public-ip-address \
        ${market_args[@]+"${market_args[@]}"} \
        ${cpu_args[@]+"${cpu_args[@]}"} \
        --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=$disk,VolumeType=gp3,Throughput=250,Iops=6000}" \
        --user-data "file://$data" \
        --tag-specifications "$(printf "$tags" instance "$name")" "$(printf "$tags" volume "$name")" \
        --query 'Instances[0].InstanceId' --output text
}

for name in "${SB_MANAGERS[@]}"; do launch "$name" "$SB_MANAGER_TYPE" 20 "$SB_STATE/node-user-data.sh" on-demand; done
for name in "${SB_WORKERS[@]}"; do launch "$name" "$SB_WORKER_TYPE" 40 "$SB_STATE/node-user-data.sh"; done
for name in ${SB_LOADERS[@]+"${SB_LOADERS[@]}"}; do launch "$name" "$SB_LOAD_TYPE" 20 "$SB_STATE/node-user-data.sh"; done
if [ -n "$with_build" ]; then
    launch sbx-build "$SB_BUILD_TYPE" 80 "$SB_DIR/cloud-init/build.sh" persistent
fi

sb_log "Waiting for instances to run"
# shellcheck disable=SC2046
aws ec2 wait instance-running --instance-ids $(sb_hosts | awk '{ print $4 }')
for name in $(sb_hosts | awk '{ print $1 }'); do
    until sb_ssh "$name" true 2>/dev/null; do sleep 5; done
    sb_ssh "$name" 'cloud-init status --wait >/dev/null; cloud-init status || [ $? -eq 2 ]' | sed "s/^/$name: /"
done
sb_hosts | tee "$SB_STATE/hosts"
