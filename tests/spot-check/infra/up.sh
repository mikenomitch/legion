#!/usr/bin/env bash
# Creates the spot-check rig: 3 on-demand managers, 2 spot workers with
# nested virtualization and an EBS data disk, two security groups, an S3
# bucket for the binary, and ECR repositories for the test images.
#
# Idempotent: re-running skips what already exists. Tear down with down.sh.
#
#   tests/spot-check/infra/up.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

admin_cidr="${SC_ADMIN_CIDR:-$(curl -fsS https://checkip.amazonaws.com)/32}"
echo "$admin_cidr" >"$SC_STATE/admin_cidr"
vpc="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)"
subnet="$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$vpc" "Name=availability-zone,Values=$SC_AZ" \
    "Name=default-for-az,Values=true" --query 'Subnets[0].SubnetId' --output text)"
tags="ResourceType=%s,Tags=[{Key=Project,Value=$SC_PROJECT},{Key=Name,Value=%s}]"

# --- SSH key
if [ ! -f "$SC_KEY" ]; then
    ssh-keygen -q -t ed25519 -N "" -C "$SC_PROJECT" -f "$SC_KEY"
fi
if ! aws ec2 describe-key-pairs --key-names "$SC_PROJECT" >/dev/null 2>&1; then
    sc_log "Importing key pair"
    aws ec2 import-key-pair --key-name "$SC_PROJECT" --public-key-material "fileb://$SC_KEY.pub" \
        --tag-specifications "$(printf "$tags" key-pair "$SC_PROJECT")" >/dev/null
fi

# --- Security groups
sg_id() {
    aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$vpc" "Name=group-name,Values=$1" \
        --query 'SecurityGroups[0].GroupId' --output text
}
ensure_sg() {
    local id
    id="$(sg_id "$1")"
    if [ "$id" = "None" ]; then
        sc_log "Creating security group $1"
        id="$(aws ec2 create-security-group --vpc-id "$vpc" --group-name "$1" --description "$2" \
            --tag-specifications "$(printf "$tags" security-group "$1")" --query GroupId --output text)"
    fi
    echo "$id"
}
allow() { # sg proto from to source(cidr|sg)
    local src
    if [[ "$5" == sg-* ]]; then src="UserIdGroupPairs=[{GroupId=$5}]"; else src="IpRanges=[{CidrIp=$5}]"; fi
    aws ec2 authorize-security-group-ingress --group-id "$1" \
        --ip-permissions "IpProtocol=$2,FromPort=$3,ToPort=$4,$src" >/dev/null 2>&1 || true
}
mgr_sg="$(ensure_sg sc-managers "legion spot-check managers")"
wkr_sg="$(ensure_sg sc-workers "legion spot-check workers")"

# Managers: SSH, the API (4747), the exec WebSocket (4748), and metrics (4749)
# from the operator; the API and raft (both 4747) from the rig.
allow "$mgr_sg" tcp 22 22 "$admin_cidr"
allow "$mgr_sg" tcp 4747 4749 "$admin_cidr"
allow "$mgr_sg" tcp 4747 4747 "$mgr_sg"
allow "$mgr_sg" tcp 4747 4747 "$wkr_sg"
# Workers: SSH and app ports from the operator. 4848 (the worker plane,
# mutual TLS) only from managers, as defense in depth. Task ports (static
# 8080-8099 and dynamic 20000-32000) are open between workers for the mesh.
allow "$wkr_sg" tcp 22 22 "$admin_cidr"
allow "$wkr_sg" tcp 8080 8099 "$admin_cidr"
allow "$wkr_sg" tcp 20000 32000 "$admin_cidr"
allow "$wkr_sg" tcp 4848 4848 "$mgr_sg"
allow "$wkr_sg" tcp 8080 8099 "$wkr_sg"
allow "$wkr_sg" tcp 20000 32000 "$wkr_sg"

# --- S3 bucket for the binary, ECR repositories for the images
if ! aws s3api head-bucket --bucket "$SC_BUCKET" >/dev/null 2>&1; then
    sc_log "Creating bucket $SC_BUCKET"
    aws s3api create-bucket --bucket "$SC_BUCKET" >/dev/null
    aws s3api put-bucket-tagging --bucket "$SC_BUCKET" \
        --tagging "TagSet=[{Key=Project,Value=$SC_PROJECT}]"
fi
for repo in web api frontend agent db; do
    aws ecr describe-repositories --repository-names "spot-check/$repo" >/dev/null 2>&1 ||
        aws ecr create-repository --repository-name "spot-check/$repo" \
            --tags "Key=Project,Value=$SC_PROJECT" >/dev/null
done

# --- Instances
ami="$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
    --query Parameter.Value --output text)"
render() {
    sed -e "s/@@RUNC_VERSION@@/$SC_RUNC_VERSION/g" -e "s/@@CH_VERSION@@/$SC_CH_VERSION/g" \
        -e "s/@@KERNEL_RELEASE@@/$SC_KERNEL_RELEASE/g" "$1" >"$2"
}
render "$SC_DIR/infra/cloud-init/manager.sh" "$SC_STATE/manager-user-data.sh"
render "$SC_DIR/infra/cloud-init/worker.sh" "$SC_STATE/worker-user-data.sh"
existing="$(sc_hosts | awk '{ print $1 }')"

for name in "${SC_MANAGERS[@]}"; do
    grep -qx "$name" <<<"$existing" && continue
    sc_log "Launching $name ($SC_MANAGER_TYPE, on-demand)"
    aws ec2 run-instances --image-id "$ami" --instance-type "$SC_MANAGER_TYPE" --key-name "$SC_PROJECT" \
        --subnet-id "$subnet" --security-group-ids "$mgr_sg" --associate-public-ip-address \
        --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=20,VolumeType=gp3}' \
        --user-data "file://$SC_STATE/manager-user-data.sh" \
        --tag-specifications "$(printf "$tags" instance "$name")" "$(printf "$tags" volume "$name")" \
        --query 'Instances[0].InstanceId' --output text
done

for name in "${SC_WORKERS[@]}"; do
    grep -qx "$name" <<<"$existing" && continue
    sc_log "Launching $name ($SC_WORKER_TYPE, spot, nested virtualization)"
    aws ec2 run-instances --image-id "$ami" --instance-type "$SC_WORKER_TYPE" --key-name "$SC_PROJECT" \
        --subnet-id "$subnet" --security-group-ids "$wkr_sg" --associate-public-ip-address \
        --instance-market-options 'MarketType=spot,SpotOptions={SpotInstanceType=one-time,InstanceInterruptionBehavior=terminate}' \
        --cpu-options 'NestedVirtualization=enabled' \
        --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=30,VolumeType=gp3}' \
        'DeviceName=/dev/sdf,Ebs={VolumeSize=20,VolumeType=gp3,DeleteOnTermination=true}' \
        --user-data "file://$SC_STATE/worker-user-data.sh" \
        --tag-specifications "$(printf "$tags" instance "$name")" "$(printf "$tags" volume "$name")" \
        --query 'Instances[0].InstanceId' --output text
done

sc_log "Waiting for instances to run"
# shellcheck disable=SC2046
aws ec2 wait instance-running --instance-ids $(sc_hosts | awk '{ print $4 }')

sc_log "Waiting for first-boot setup (cloud-init) on every box"
for name in "${SC_MANAGERS[@]}" "${SC_WORKERS[@]}"; do
    until sc_ssh "$name" true 2>/dev/null; do sleep 5; done
    # `cloud-init status` exits 2 on "degraded done" (a recoverable error,
    # such as a slow metadata service at boot), which is not a failed setup.
    sc_ssh "$name" 'cloud-init status --wait >/dev/null; cloud-init status || [ $? -eq 2 ]' | sed "s/^/$name: /"
done

sc_hosts | tee "$SC_STATE/hosts"
sc_log "Rig is up. Next: tests/spot-check/infra/install.sh"
