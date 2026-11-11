#!/usr/bin/env bash
# Deletes the sandbox-boot rig: every instance tagged Project=legion-sbx-bench
# (cancelling the build box's persistent spot request first, or AWS relaunches
# it), then the security group. The bucket and key pair are kept; pass
# `--all` to delete those too.
set -euo pipefail
source "$(dirname "$0")/env.sh"

requests="$(aws ec2 describe-spot-instance-requests \
    --filters "Name=tag:Project,Values=$SB_PROJECT" "Name=state,Values=open,active,disabled" \
    --query 'SpotInstanceRequests[].SpotInstanceRequestId' --output text)"
# Spot requests inherit no tags from run-instances; find the build box's by instance.
for id in $(aws ec2 describe-instances --filters "Name=tag:Project,Values=$SB_PROJECT" \
    --query 'Reservations[].Instances[].SpotInstanceRequestId' --output text); do
    requests="$requests $id"
done
if [ -n "${requests// /}" ]; then
    # shellcheck disable=SC2086
    aws ec2 cancel-spot-instance-requests --spot-instance-request-ids $requests >/dev/null || true
fi

ids="$(aws ec2 describe-instances --filters "Name=tag:Project,Values=$SB_PROJECT" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].InstanceId' --output text)"
if [ -n "$ids" ]; then
    sb_log "Terminating $ids"
    # shellcheck disable=SC2086
    aws ec2 terminate-instances --instance-ids $ids >/dev/null
    # shellcheck disable=SC2086
    aws ec2 wait instance-terminated --instance-ids $ids
fi

vpc="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)"
sg="$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$vpc" "Name=group-name,Values=$SB_PROJECT" \
    --query 'SecurityGroups[0].GroupId' --output text)"
[ "$sg" != "None" ] && aws ec2 delete-security-group --group-id "$sg" || true

if [ "${1:-}" = "--all" ]; then
    aws s3 rb "s3://$SB_BUCKET" --force >/dev/null 2>&1 || true
    aws ec2 delete-key-pair --key-name "$SB_PROJECT" >/dev/null 2>&1 || true
fi
sb_log "Rig is down"
