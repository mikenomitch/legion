#!/usr/bin/env bash
# Tear the whole rig down before it costs real money. Destroys every
# instance AND the S3 bucket (force_destroy) — campaign archives you
# have not synced out die with it, which is why this script syncs them
# first unless told not to.
#
# Usage: teardown.sh [--no-sync] [--yes]
set -euo pipefail
source "$(dirname "$0")/env.sh"

SYNC=true CONFIRM=true
for arg in "$@"; do
    case "$arg" in
    --no-sync) SYNC=false ;;
    --yes) CONFIRM=false ;;
    esac
done

if load_rig 2>/dev/null; then
    if $SYNC; then
        # Durable copy first. collect.sh already mirrors each cell as it
        # finishes; this catches any whose mirror failed, and it is the
        # only copy that survives both this destroy and the machine
        # running it.
        if [ -n "${ARCHIVE_BUCKET:-}" ]; then
            log "syncing s3://$BUCKET/campaigns/ -> s3://$ARCHIVE_BUCKET/campaigns/ (survives teardown)"
            aws s3 sync --region "$REGION" \
                "s3://$BUCKET/campaigns/" "s3://$ARCHIVE_BUCKET/campaigns/" ||
                log "WARNING: archive-bucket sync failed — do NOT proceed if you need these results"
            log "syncing status stream -> s3://$ARCHIVE_BUCKET/status/"
            aws s3 sync --region "$REGION" \
                "s3://$BUCKET/status/" "s3://$ARCHIVE_BUCKET/status/" || true
        else
            log "WARNING: no archive_bucket configured."
            log "         The only copies after this destroy are the local ones below, on THIS machine."
            log "         If this machine is disposable, set archive_bucket (terraform/variables.tf) and re-run."
        fi

        log "syncing campaign archives from s3://$BUCKET/campaigns/ to $RESULTS_ROOT/s3-mirror/"
        mkdir -p "$RESULTS_ROOT/s3-mirror"
        aws s3 sync --region "$REGION" "s3://$BUCKET/campaigns/" "$RESULTS_ROOT/s3-mirror/" || true
    fi
fi

if $CONFIRM; then
    read -r -p "Destroy the entire benchmark rig (instances + bucket)? [y/N] " answer
    [[ "$answer" == [yY]* ]] || {
        echo "aborted"
        exit 1
    }
fi

terraform -chdir="$TF_DIR" destroy -auto-approve -input=false
log "rig destroyed"
