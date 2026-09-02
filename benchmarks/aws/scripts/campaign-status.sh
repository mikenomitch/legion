#!/usr/bin/env bash
# Campaign status, published to and read from S3.
#
# `remote-campaign.sh status` answers "is it running?" over SSH, from an
# address inside admin_cidr, against a box that has to still exist. None
# of those hold for the thing most likely to be watching an overnight
# campaign: an agent or a cloud shell somewhere else. Those have HTTPS
# and AWS credentials and nothing else — commonly not even outbound port
# 22.
#
# So the campaign publishes its own state to S3 and anyone reads it from
# there. `show` needs a bucket name, a region, and credentials; it does
# not need the repo, terraform state, the rig manifest, SSH, or the rig
# to still be standing.
#
#   publish  — called by the campaign itself at each transition, plus on
#              a heartbeat, so a stalled run is distinguishable from a
#              finished one.
#   show     — one-shot read. Exit code IS the state, for polling:
#              0 running, 2 finished ok, 3 failed, 4 stale, 5 missing.
#   watch    — poll show until it reaches a terminal state.
#
# Usage:
#   campaign-status.sh publish --campaign <id> --phase <phase> \
#       [--detail <text>] [--step <n>] [--of <n>] [--log <file>]
#   campaign-status.sh show  [--bucket <b>] [--region <r>] [--campaign <id>]
#   campaign-status.sh watch [--bucket <b>] [--region <r>] [--interval 60]
#
# Off-rig with terraform state, --bucket and --region are inferred.
set -euo pipefail

ACTION="${1:-}"
shift || true

# `show`/`watch` must work with nothing but a bucket name, so env.sh —
# which wants terraform or a manifest — is sourced only when it can be.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUCKET="${BENCH_BUCKET:-}" REGION="${BENCH_REGION:-${AWS_REGION:-}}"
CAMPAIGN="" PHASE="" DETAIL="" STEP="" OF="" LOGFILE="" INTERVAL=60
STALE_AFTER=1800

while [ $# -gt 0 ]; do
    case "$1" in
    --bucket) BUCKET="$2" ;;
    --region) REGION="$2" ;;
    --campaign) CAMPAIGN="$2" ;;
    --phase) PHASE="$2" ;;
    --detail) DETAIL="$2" ;;
    --step) STEP="$2" ;;
    --of) OF="$2" ;;
    --log) LOGFILE="$2" ;;
    --interval) INTERVAL="$2" ;;
    --stale-after) STALE_AFTER="$2" ;;
    *)
        echo "unknown arg $1" >&2
        exit 64
        ;;
    esac
    shift 2
done

# An explicit --bucket wins and keeps this script dependency-free. Only
# when one wasn't given do we ask the rig where it lives — env.sh sets
# BUCKET/REGION itself, hence the save-and-restore around it.
if [ -z "$BUCKET" ]; then
    _want_region="$REGION"
    # shellcheck source=env.sh
    if source "$SCRIPT_DIR/env.sh" 2>/dev/null && load_rig 2>/dev/null; then
        REGION="${_want_region:-$REGION}"
    fi
fi
: "${BUCKET:?--bucket is required (no terraform state or rig manifest here to infer it from)}"
: "${REGION:=us-east-1}"

STATUS_PREFIX="s3://$BUCKET/status"

publish() {
    : "${CAMPAIGN:?--campaign required}" "${PHASE:?--phase required}"
    local now doc
    now=$(date -u +%FT%TZ)
    doc=$(jq -n \
        --arg campaign "$CAMPAIGN" --arg phase "$PHASE" --arg detail "$DETAIL" \
        --arg updated_at "$now" --arg host "$(hostname)" \
        --argjson step "${STEP:-null}" --argjson of "${OF:-null}" \
        '{campaign: $campaign, phase: $phase, detail: $detail,
          step: $step, of: $of, updated_at: $updated_at, published_by: $host}')

    # Two keys on purpose: the per-campaign one is the record, and
    # `latest.json` is what a watcher with no campaign id starts from.
    echo "$doc" | aws s3 cp --region "$REGION" - "$STATUS_PREFIX/$CAMPAIGN/status.json" --quiet
    echo "$doc" | aws s3 cp --region "$REGION" - "$STATUS_PREFIX/latest.json" --quiet

    # The console log rides along, so a watcher can read the actual
    # failure instead of only learning that there was one.
    if [ -n "$LOGFILE" ] && [ -f "$LOGFILE" ]; then
        tail -c 2000000 "$LOGFILE" |
            aws s3 cp --region "$REGION" - "$STATUS_PREFIX/$CAMPAIGN/console.log" --quiet || true
    fi
}

fetch_status() { # fetch_status -> doc on stdout, or empty
    local key="$STATUS_PREFIX/latest.json"
    [ -z "$CAMPAIGN" ] || key="$STATUS_PREFIX/$CAMPAIGN/status.json"
    aws s3 cp --region "$REGION" "$key" - 2>/dev/null || true
}

show() {
    local doc age now updated phase
    doc=$(fetch_status)
    if [ -z "$doc" ]; then
        echo "no campaign status in s3://$BUCKET/status/ — none has run, or it predates status publishing"
        return 5
    fi

    updated=$(jq -r '.updated_at' <<<"$doc")
    now=$(date -u +%s)
    age=$((now - $(date -u -d "$updated" +%s 2>/dev/null || date -u -j -f "%Y-%m-%dT%H:%M:%SZ" "$updated" +%s 2>/dev/null || echo "$now")))
    phase=$(jq -r '.phase' <<<"$doc")

    jq -r '"campaign: \(.campaign)\nphase:    \(.phase)" +
           (if .step and .of then "\nstep:     \(.step)/\(.of)" else "" end) +
           (if .detail != "" then "\ndetail:   \(.detail)" else "" end) +
           "\nupdated:  \(.updated_at)"' <<<"$doc"
    echo "age:      ${age}s"
    echo "results:  s3://$BUCKET/campaigns/  (archive mirror if configured)"
    echo "console:  aws s3 cp s3://$BUCKET/status/$(jq -r .campaign <<<"$doc")/console.log -"

    case "$phase" in
    succeeded) return 2 ;;
    failed) return 3 ;;
    esac
    # A running campaign that stopped heartbeating is the case a watcher
    # most needs told apart from a healthy one: the box may be gone.
    if [ "$age" -gt "$STALE_AFTER" ]; then
        echo "STALE: no heartbeat in ${age}s (> ${STALE_AFTER}s) — the driver or its box may be gone"
        return 4
    fi
    return 0
}

case "$ACTION" in
publish) publish ;;
show) show ;;
watch)
    while :; do
        set +e
        show
        code=$?
        set -e
        [ "$code" = 0 ] || exit "$code"
        sleep "$INTERVAL"
    done
    ;;
*)
    sed -n '2,30p' "${BASH_SOURCE[0]}" >&2
    exit 64
    ;;
esac
