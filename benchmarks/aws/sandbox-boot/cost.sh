#!/usr/bin/env bash
# Estimates what the rig has cost so far: every instance tagged
# Project=legion-sbx-bench that ran at some point (the ledger in .state keeps
# terminated ones), priced at the current spot price for its type and AZ, plus
# EBS and public IPv4 at list price. An estimate, not a bill.
#
#   benchmarks/aws/sandbox-boot/cost.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

ledger="$SB_STATE/ledger.tsv" # id type az launched_epoch ended_epoch disk_gb
touch "$ledger"
now="$(date -u +%s)"

# Record or refresh every box the account still knows about.
aws ec2 describe-instances --filters "Name=tag:Project,Values=$SB_PROJECT" \
    --query 'Reservations[].Instances[].[InstanceId,InstanceType,Placement.AvailabilityZone,LaunchTime,State.Name,BlockDeviceMappings[0].Ebs.VolumeId]' \
    --output text | while read -r id type az launched state _vol; do
    start="$(date -u -j -f '%Y-%m-%dT%H:%M:%S' "${launched%%+*}" +%s 2>/dev/null || date -u -d "$launched" +%s)"
    end="$now"
    case "$state" in terminated | shutting-down | stopped) end="$(awk -v i="$id" '$1 == i { print $5 }' "$ledger")"; [ -z "$end" ] && end="$now" ;; esac
    disk=$(case "$type" in "$SB_BUILD_TYPE") echo 80 ;; "$SB_WORKER_TYPE") echo 40 ;; *) echo 20 ;; esac)
    grep -v "^$id	" "$ledger" >"$ledger.new" || true
    # A stopped or terminated box keeps the end time it was first seen with.
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$type" "$az" "$start" "$end" "$disk" >>"$ledger.new"
    mv "$ledger.new" "$ledger"
done

total=0
while IFS=$'\t' read -r id type az start end disk; do
    price="$(aws ec2 describe-spot-price-history --instance-types "$type" --availability-zone "$az" \
        --product-descriptions Linux/UNIX --start-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
        --query 'SpotPriceHistory[0].SpotPrice' --output text)"
    hours="$(echo "($end - $start) / 3600" | bc -l)"
    # gp3 at $0.08/GB-month, public IPv4 at $0.005/hour.
    cost="$(echo "$hours * ($price + 0.005 + $disk * 0.08 / 730)" | bc -l)"
    printf '%-20s %-12s %6.2fh  $%.3f\n' "$id" "$type" "$hours" "$cost"
    total="$(echo "$total + $cost" | bc -l)"
done <"$ledger"
printf 'Total so far: $%.2f\n' "$total"
