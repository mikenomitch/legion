#!/usr/bin/env bash
# Deletes everything up.sh created: instances (and their volumes), security
# groups, the key pair, the S3 bucket, and the ECR repositories. Only
# resources tagged or named for the spot check are touched.
#
#   tests/spot-check/infra/down.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

ids="$(sc_hosts | awk '{ print $4 }')"
stopped="$(aws ec2 describe-instances \
    --filters "Name=tag:Project,Values=$SC_PROJECT" "Name=instance-state-name,Values=stopping,stopped" \
    --query 'Reservations[].Instances[].InstanceId' --output text)"
ids="$(echo $ids $stopped)"
if [ -n "$ids" ]; then
    sc_log "Terminating $ids"
    # shellcheck disable=SC2086
    aws ec2 terminate-instances --instance-ids $ids >/dev/null
    # shellcheck disable=SC2086
    aws ec2 wait instance-terminated --instance-ids $ids
fi

vpc="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)"
mgr_sg="$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$vpc" Name=group-name,Values=sc-managers \
    --query 'SecurityGroups[0].GroupId' --output text)"
wkr_sg="$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$vpc" Name=group-name,Values=sc-workers \
    --query 'SecurityGroups[0].GroupId' --output text)"
# The groups reference each other, so drop the rules before the groups.
for sg in "$mgr_sg" "$wkr_sg"; do
    [ "$sg" = "None" ] && continue
    perms="$(aws ec2 describe-security-groups --group-ids "$sg" --query 'SecurityGroups[0].IpPermissions' --output json)"
    [ "$perms" != "[]" ] && aws ec2 revoke-security-group-ingress --group-id "$sg" --ip-permissions "$perms" >/dev/null
done
for sg in "$mgr_sg" "$wkr_sg"; do
    [ "$sg" = "None" ] && continue
    sc_log "Deleting security group $sg"
    aws ec2 delete-security-group --group-id "$sg"
done

aws ec2 delete-key-pair --key-name "$SC_PROJECT" >/dev/null 2>&1 || true

if aws s3api head-bucket --bucket "$SC_BUCKET" >/dev/null 2>&1; then
    sc_log "Deleting bucket $SC_BUCKET"
    aws s3 rb "s3://$SC_BUCKET" --force >/dev/null
fi
for repo in web api frontend agent db; do
    aws ecr delete-repository --repository-name "spot-check/$repo" --force >/dev/null 2>&1 || true
done

rm -f "$SC_STATE/hosts" "$SC_STATE/token" "$SC_STATE/known_hosts"
sc_log "Rig is gone."
