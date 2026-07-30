#!/usr/bin/env bash
# Rolling manager upgrade under load (Legion A), per docs/upgrades.md:
# managers restarted one at a time onto a new binary while the driver
# watches placements; the disruption number is the placement gap around
# each restart. The "new" binary defaults to the current S3 one (a
# same-version roll measures pure restart disruption; upload a newer
# build first to measure a real upgrade).
#
# Usage: upgrade-under-load.sh [--binary s3://bucket/bin/legion] [--campaign <id>]
set -euo pipefail
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../scripts" && pwd)"
source "$SCRIPTS/env.sh"
load_rig

CAMPAIGN="" NEW_BINARY=""
while [ $# -gt 0 ]; do
    case "$1" in
    --binary) NEW_BINARY="$2" ;;
    --campaign) CAMPAIGN="$2" ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
    shift 2
done
CAMPAIGN="${CAMPAIGN:-adhoc-$(date -u +%Y%m%d-%H%M%S)}"
NEW_BINARY="${NEW_BINARY:-s3://$BUCKET/bin/legion}"
MANAGER="http://${CP_PRIVATE_IPS[0]}:4747"

RUN_ID="experiments/upgrade-$(date -u +%Y%m%d-%H%M%S)"
REMOTE_OUT="/opt/bench/runs/$CAMPAIGN/$RUN_ID"

# The rolling upgrade, as one fault command the watch can time: for each
# manager — swap binary, restart, wait ready — in series, exactly the
# docs/upgrades.md order (followers first, leader last would need leader
# detection; we roll cp-2, cp-1, cp-0 which demotes the common bootstrap
# leader last).
UPGRADE_CMD=""
for idx in 2 1 0; do
    UPGRADE_CMD+="ssh -o StrictHostKeyChecking=accept-new ubuntu@${CP_PRIVATE_IPS[$idx]} '
        sudo aws s3 cp --region $REGION $NEW_BINARY /opt/legion/bin/legion.new &&
        sudo chmod +x /opt/legion/bin/legion.new &&
        sudo mv /opt/legion/bin/legion.new /opt/legion/bin/legion &&
        sudo systemctl restart legion-manager'; "
    UPGRADE_CMD+="until curl -fsS -m 3 http://${CP_PRIVATE_IPS[$idx]}:4749/healthz >/dev/null 2>&1; do sleep 2; done; "
done

rsh "$LOADGEN_PUBLIC_IP" "
    set -euo pipefail
    mkdir -p '$REMOTE_OUT'
    # Trickle load so gaps are observable (same shape as the failover drill).
    python3 /opt/bench/workloads/translate.py \
        --scenario /opt/bench/scenarios/steady_small.json \
        --system legion --count-multiplier 10 \
        --out '$REMOTE_OUT/trickle' >/dev/null
    (
      for f in '$REMOTE_OUT'/trickle/*.jsonnet; do
        LEGION_ADDR='$MANAGER' legion job run \"\$f\" -j >/dev/null 2>&1 || true
        sleep 3
      done
    ) &
    TRICKLE_PID=\$!
    python3 /opt/bench/driver/driver.py watch \
        --system legion \
        --manager '$MANAGER' \
        --out '$REMOTE_OUT/run' \
        --exec-after-secs 15 \
        --exec-cmd \"$UPGRADE_CMD\" \
        --settle-timeout-secs 300 \
        --legion-bin /usr/local/bin/legion \
        | tee '$REMOTE_OUT/metrics.json'
    kill \$TRICKLE_PID 2>/dev/null || true
"

"$SCRIPTS/collect.sh" --campaign "$CAMPAIGN" --run "$RUN_ID" --system legion-a \
    --nodes 0 --scenario upgrade
log "upgrade drill complete: placement_gap_secs in $RESULTS_ROOT/$CAMPAIGN/$RUN_ID/metrics.json"
