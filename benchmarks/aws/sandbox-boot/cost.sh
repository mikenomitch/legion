#!/usr/bin/env bash
# Estimates what the rig has cost so far: every instance tagged
# Project=legion-sbx-bench, priced at the current spot price for its type and
# AZ, plus gp3 and public IPv4 at list price. A ledger in .state keeps boxes
# AWS has forgotten (terminated ones drop out of describe-instances after an
# hour) and the last time each was seen running. An estimate, not a bill.
#
#   benchmarks/aws/sandbox-boot/cost.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

ledger="$SB_STATE/ledger.json"
[ -s "$ledger" ] || echo '{}' >"$ledger"

aws ec2 describe-instances --filters "Name=tag:Project,Values=$SB_PROJECT" --output json \
    --query 'Reservations[].Instances[].{id:InstanceId,type:InstanceType,az:Placement.AvailabilityZone,launched:LaunchTime,state:State.Name}' \
    >"$SB_STATE/instances.json"

python3 - "$ledger" "$SB_STATE/instances.json" <<'EOF' >"$ledger.new"
import json, sys, time
from datetime import datetime
ledger = json.load(open(sys.argv[1]))
now = time.time()
for box in json.load(open(sys.argv[2])):
    entry = ledger.setdefault(box["id"], {"type": box["type"], "az": box["az"]})
    launched = datetime.fromisoformat(box["launched"]).timestamp()
    entry["start"] = min(entry.get("start", launched), launched)
    # Billing stops when a box stops or terminates; the last time it was
    # seen running is the best estimate of when that happened. A stopped box
    # started again is counted across the gap, which overstates its cost.
    if box["state"] in ("running", "pending"):
        entry["end"] = now
    entry.setdefault("end", now)
json.dump(ledger, sys.stdout, indent=1)
EOF
mv "$ledger.new" "$ledger"

total=0
for id in $(jq -r 'keys[]' "$ledger"); do
    read -r type az start end <<<"$(jq -r --arg id "$id" '.[$id] | "\(.type) \(.az) \(.start) \(.end)"' "$ledger")"
    price="$(aws ec2 describe-spot-price-history --instance-types "$type" --availability-zone "$az" \
        --product-descriptions Linux/UNIX --start-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
        --query 'SpotPriceHistory[0].SpotPrice' --output text)"
    disk=20
    [ "$type" = "$SB_BUILD_TYPE" ] && disk=80
    [ "$type" = "$SB_WORKER_TYPE" ] && disk=40
    hours="$(echo "($end - $start) / 3600" | bc -l)"
    # gp3 at $0.08/GB-month, public IPv4 at $0.005/hour.
    cost="$(echo "$hours * ($price + 0.005 + $disk * 0.08 / 730)" | bc -l)"
    printf '%-20s %-12s %6.2fh  $%.3f\n' "$id" "$type" "$hours" "$cost"
    total="$(echo "$total + $cost" | bc -l)"
done
printf 'Total so far: $%.2f (of the $10 budget)\n' "$total"
