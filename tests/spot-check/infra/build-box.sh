#!/usr/bin/env bash
# A temporary c7i build box for the rig's Linux binaries: about four minutes
# per build instead of an hour under Rosetta, and the binary goes straight to
# the rig's S3 bucket instead of crossing your laptop's link twice.
#
#   tests/spot-check/infra/build-box.sh launch      # first time; then `wait`
#   tests/spot-check/infra/build-box.sh wait
#   tests/spot-check/infra/build-box.sh sync        # this checkout (or SRC=<dir>)
#   tests/spot-check/infra/build-box.sh build       # legion + static legion-guest
#   tests/spot-check/infra/build-box.sh push        # build box -> s3://<rig bucket>
#   tests/spot-check/infra/build-box.sh deploy      # rig pulls from S3, restarts
#   tests/spot-check/infra/build-box.sh stop        # keeps the cargo cache (EBS only)
#   tests/spot-check/infra/build-box.sh start | ssh <cmd> | terminate
#
# Tagged Project=legion-spot-check, so down.sh deletes it too.
set -euo pipefail
source "$(dirname "$0")/env.sh"

NAME=sc-build
TYPE="${SC_BUILD_TYPE:-c7i.8xlarge}"
SRC="${SRC:-$SC_REPO}"

bb_id() {
    aws ec2 describe-instances --filters "Name=tag:Name,Values=$NAME" "Name=tag:Project,Values=$SC_PROJECT" \
        "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query 'Reservations[].Instances[].InstanceId' --output text
}
bb_ip() {
    aws ec2 describe-instances --filters "Name=tag:Name,Values=$NAME" "Name=instance-state-name,Values=running" \
        --query 'Reservations[].Instances[].PublicIpAddress' --output text
}
bb_ssh() {
    ssh -i "$SC_KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$SC_STATE/known_hosts" \
        -o ConnectTimeout=10 -o LogLevel=ERROR -o ServerAliveInterval=30 "ubuntu@$(bb_ip)" "$@"
}

case "${1:-}" in
launch)
    vpc="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)"
    subnet="$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$vpc" "Name=availability-zone,Values=$SC_AZ" \
        "Name=default-for-az,Values=true" --query 'Subnets[0].SubnetId' --output text)"
    # The managers' group already admits SSH from the operator.
    sg="$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$vpc" Name=group-name,Values=sc-managers \
        --query 'SecurityGroups[0].GroupId' --output text)"
    ami="$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
        --query Parameter.Value --output text)"
    cat >"$SC_STATE/build-user-data.sh" <<'EOF'
#!/bin/bash
set -eux
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q build-essential pkg-config protobuf-compiler clang libclang-dev musl-tools rsync jq
snap install aws-cli --classic
sudo -u ubuntu bash -c 'curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.90.0 --profile minimal && ~/.cargo/bin/rustup target add x86_64-unknown-linux-musl'
touch /var/tmp/build-ready
EOF
    aws ec2 run-instances --image-id "$ami" --instance-type "$TYPE" --key-name "$SC_PROJECT" \
        --subnet-id "$subnet" --security-group-ids "$sg" --associate-public-ip-address \
        --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=80,VolumeType=gp3,Throughput=500,Iops=6000}' \
        --user-data "file://$SC_STATE/build-user-data.sh" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Project,Value=$SC_PROJECT},{Key=Name,Value=$NAME}]" \
        --query 'Instances[0].InstanceId' --output text
    ;;
start) aws ec2 start-instances --instance-ids "$(bb_id)" >/dev/null && "$0" wait ;;
stop) aws ec2 stop-instances --instance-ids "$(bb_id)" >/dev/null && echo stopping ;;
wait)
    until [ -n "$(bb_ip)" ] && bb_ssh test -f /var/tmp/build-ready 2>/dev/null; do sleep 10; done
    echo ready
    ;;
sync)
    # Only what git sees (tracked plus untracked-but-not-ignored), minus the
    # UI and stray build dirs. Stale files on the box are removed first.
    files="$(mktemp)"
    git -C "$SRC" ls-files -co --exclude-standard | grep -v -E '^(ui/|target)' >"$files"
    bb_ssh 'mkdir -p legion && cd legion && rm -rf src tests docs proto guest configs internal_docs plugins'
    rsync -az -e "ssh -i $SC_KEY -o UserKnownHostsFile=$SC_STATE/known_hosts -o LogLevel=ERROR" \
        --files-from="$files" "$SRC/" "ubuntu@$(bb_ip):legion/"
    rm -f "$files"
    ;;
build)
    bb_ssh 'cd legion && source ~/.cargo/env && cargo build --release --locked --bin legion &&
        cargo build -p legion-guest --profile dist --locked --target x86_64-unknown-linux-musl'
    ;;
push)
    # Short-lived session credentials, passed for this one command only.
    creds="$(aws sts get-session-token --duration-seconds 3600 --output json)"
    bb_ssh "export AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$creds") \
        AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$creds") \
        AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$creds") AWS_DEFAULT_REGION=$AWS_REGION &&
        aws s3 cp --quiet legion/target/release/legion s3://$SC_BUCKET/legion &&
        aws s3 cp --quiet legion/target/x86_64-unknown-linux-musl/dist/legion-guest s3://$SC_BUCKET/legion-guest &&
        echo pushed"
    ;;
deploy)
    # install.sh's SC_ONLY_BINARY path, from what `push` left in S3: managers
    # first (followers before the leader), then workers (docs/upgrades.md).
    url="$(aws s3 presign "s3://$SC_BUCKET/legion" --expires-in 3600)"
    gurl="$(aws s3 presign "s3://$SC_BUCKET/legion-guest" --expires-in 3600)"
    for name in "${SC_MANAGERS[@]}" "${SC_WORKERS[@]}"; do
        sc_ssh "$name" "sudo curl -fsS -o /opt/legion/bin/legion.new '$url' && sudo install -m 0755 /opt/legion/bin/legion.new /opt/legion/bin/legion && sudo rm /opt/legion/bin/legion.new" &
    done
    for name in "${SC_WORKERS[@]}"; do
        sc_ssh "$name" "sudo curl -fsS -o /opt/legion/bin/legion-guest.new '$gurl' && sudo install -m 0755 /opt/legion/bin/legion-guest.new /opt/legion/bin/legion-guest && sudo rm /opt/legion/bin/legion-guest.new" &
    done
    wait
    leader="$("$SC_DIR/infra/leader.sh")"
    for name in "${SC_MANAGERS[@]}"; do
        [ "$name" = "$leader" ] && continue
        sc_ssh "$name" sudo systemctl restart legion-manager
        sleep 5
    done
    if [ -n "$leader" ]; then sc_ssh "$leader" sudo systemctl restart legion-manager; sleep 5; fi
    for name in "${SC_WORKERS[@]}"; do sc_ssh "$name" sudo systemctl restart legion-worker; done
    sc_log "Deployed $(sc_ssh "${SC_MANAGERS[0]}" legion --version)"
    ;;
ssh) shift; bb_ssh "$@" ;;
terminate) aws ec2 terminate-instances --instance-ids "$(bb_id)" >/dev/null && echo terminated ;;
*) sed -n '2,15p' "$0"; exit 1 ;;
esac
