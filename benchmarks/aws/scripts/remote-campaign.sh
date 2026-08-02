#!/usr/bin/env bash
# Run a campaign FROM the loadgen box, so it survives your laptop going
# to sleep, your ssh session dropping, and your IP changing.
#
# Driving a campaign from your machine means an unbroken hours-long chain
# of ssh from one address: the rig's security group pins ingress to
# admin_cidr (terraform/security.tf), so a changing egress IP breaks the
# next hop, and a closed laptop strands the rig running at full cost.
# This script moves the driver's seat inside the VPC. Once `launch`
# returns, nothing on your side needs to stay up.
#
#   remote-campaign.sh launch --system legion-a --smoke
#   remote-campaign.sh launch --system legion-a         # the real thing
#   remote-campaign.sh exec experiments/soak-24h.sh --hours 24
#   remote-campaign.sh status                           # is it alive?
#   remote-campaign.sh logs -f                          # tail it
#   remote-campaign.sh fetch                            # pull results back
#   remote-campaign.sh stop                             # abort the run
#
# Run this from your machine, with terraform state and the rig ssh key
# (bringup.sh + build-and-upload.sh + the system's install script first).
set -euo pipefail
source "$(dirname "$0")/env.sh"

[ "$BENCH_ON_RIG" = 1 ] && {
    echo "already on the loadgen box — call scripts/run-campaign.sh directly" >&2
    exit 1
}

SESSION=campaign
RIG_DIR=/opt/bench/rig
LOG=/opt/bench/campaign.log
RUNNER=/opt/bench/campaign-cmd.sh

usage() {
    # The header comment IS the usage text: print it back, stripped, up
    # to the first line of code. awk rather than sed because BSD sed
    # (macOS, where operators run this) has no \? and no easy "until".
    awk 'NR>1 { if (!/^#/) exit; sub(/^# ?/, ""); print }' "$0"
    exit "${1:-1}"
}

# The private key matching var.ssh_public_key. It has to reach the
# loadgen box: that box drives the campaign over ssh, and the drills in
# experiments/ have always needed it there to inject faults. Override
# with --ssh-key or BENCH_SSH_KEY.
resolve_key() {
    resolve_local_ssh_key || {
        echo "no ssh private key found — pass --ssh-key <path>" >&2
        exit 1
    }
}

# Stage everything the on-rig scripts read: the rig tree, the topology
# manifest (which replaces terraform state), and the key.
stage() {
    local key="$1"
    load_rig

    log "staging rig tree -> $LOADGEN_ADDR:$RIG_DIR"
    rsh "$LOADGEN_ADDR" "mkdir -p $RIG_DIR /opt/bench/runs"
    tar -C "$AWS_BENCH_ROOT" \
        --exclude=results --exclude=.terraform --exclude='terraform.tfstate*' \
        --exclude=__pycache__ -czf - . |
        rsh "$LOADGEN_ADDR" "tar -C $RIG_DIR -xzf -"

    log "writing rig manifest (no terraform state on the rig)"
    write_rig_manifest | rsh "$LOADGEN_ADDR" "cat > $RIG_MANIFEST"

    log "installing rig ssh key"
    rcp "$key" "$LOADGEN_ADDR:$RIG_KEY_ON_LOADGEN"
    rsh "$LOADGEN_ADDR" "chmod 600 $RIG_KEY_ON_LOADGEN"

    rsh "$LOADGEN_ADDR" "command -v tmux >/dev/null || (sudo apt-get update -qq && sudo apt-get install -y -qq tmux)"
}

# Start <command...> detached, logging to $LOG. tmux (not nohup) so you
# can attach to a running campaign and watch it work.
#
# The command goes into a staged runner script rather than being inlined
# into the ssh -> tmux -> sh chain: three levels of quoting around a
# pipeline is exactly where an overnight run breaks at 02:00. Arguments
# are printf %q-escaped on the way in.
launch_detached() {
    load_rig
    if rsh "$LOADGEN_ADDR" "tmux has-session -t $SESSION 2>/dev/null"; then
        echo "a campaign is already running — 'status', or 'stop' it first" >&2
        exit 1
    fi

    # One id for the status stream. `launch` hands the same one to
    # run-campaign.sh (--campaign) so the S3 status and the S3 archives
    # agree on what to call this run; `exec` has no campaign of its own,
    # so the id names the drill instead.
    STATUS_ID="${STATUS_ID:-$(date -u +%Y%m%d-%H%M%S)-remote}"

    local quoted=""
    local arg
    for arg in "$@"; do quoted+="$(printf '%q ' "$arg")"; done

    rsh "$LOADGEN_ADDR" "cat > $RUNNER" <<RUNNER_EOF
#!/usr/bin/env bash
# Written by scripts/remote-campaign.sh. Regenerated on every launch.
cd $RIG_DIR || exit 1
echo "=== $(date -u +%FT%TZ) launching: $quoted" >>$LOG

# Publish state to S3 as well as tmux. tmux answers "is it running?" only
# to something that can SSH in from inside admin_cidr, to a box that
# still exists — none of which is true of whatever is checking on an
# overnight run from elsewhere. S3 needs only HTTPS and credentials.
# Always best-effort: status publishing must never fail a campaign.
status() { # status <phase> [detail]
    scripts/campaign-status.sh publish --campaign '$STATUS_ID' \\
        --phase "\$1" --detail "\${2:-}" --log $LOG >/dev/null 2>&1 || true
}
status running 'started'

# The heartbeat is what separates "quiet but working" from "the box is
# gone" — a scale step can run for many minutes with no output at all.
while :; do
    sleep 60
    status running 'in progress'
done &
HEARTBEAT=\$!

set -o pipefail
$quoted 2>&1 | tee -a $LOG
code=\${PIPESTATUS[0]}
kill \$HEARTBEAT 2>/dev/null || true
echo "=== finished with exit \$code" >>$LOG
if [ "\$code" = 0 ]; then
    status succeeded 'campaign complete'
else
    status failed "exit \$code"
fi
exit \$code
RUNNER_EOF

    rsh "$LOADGEN_ADDR" "chmod +x $RUNNER && tmux new-session -d -s $SESSION 'bash $RUNNER'"
    log "launched: $quoted"
    log "it is now independent of this machine. Check on it with:"
    log "  scripts/remote-campaign.sh status"
    log "  scripts/remote-campaign.sh logs -f"
}

ACTION="${1:-}"
shift || usage
case "$ACTION" in
launch)
    KEY=""
    # Pull --ssh-key out; everything else is run-campaign.sh's.
    ARGS=()
    while [ $# -gt 0 ]; do
        case "$1" in
        --ssh-key)
            KEY="$2"
            shift 2
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
        esac
    done
    [ -n "$KEY" ] || KEY=$(resolve_key)
    stage "$KEY"
    # Shared id: the status stream and the result archives name the run
    # the same thing, so a watcher reading status.json knows exactly
    # which s3://.../campaigns/<id>/ prefix to pull.
    SYSTEM_FOR_ID=""
    for i in "${!ARGS[@]}"; do
        [ "${ARGS[$i]}" = "--system" ] && SYSTEM_FOR_ID="${ARGS[$((i + 1))]:-}"
    done
    STATUS_ID="$(date -u +%Y%m%d-%H%M)-${SYSTEM_FOR_ID:-campaign}"
    export STATUS_ID
    launch_detached "scripts/run-campaign.sh" "${ARGS[@]}" --campaign "$STATUS_ID"
    ;;

exec)
    [ $# -gt 0 ] || usage
    KEY=""
    ARGS=()
    while [ $# -gt 0 ]; do
        case "$1" in
        --ssh-key)
            KEY="$2"
            shift 2
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
        esac
    done
    [ -n "$KEY" ] || KEY=$(resolve_key)
    stage "$KEY"
    launch_detached "${ARGS[@]}"
    ;;

status)
    load_rig
    if rsh "$LOADGEN_ADDR" "tmux has-session -t $SESSION 2>/dev/null"; then
        log "campaign RUNNING on $LOADGEN_ADDR"
        rsh "$LOADGEN_ADDR" "tail -5 $LOG 2>/dev/null" || true
    else
        # No session: either it finished (the runner logs its exit code)
        # or it never started.
        outcome=$(rsh "$LOADGEN_ADDR" "grep '^=== finished with exit' $LOG 2>/dev/null | tail -1" || true)
        if [ -n "$outcome" ]; then
            case "$outcome" in
            *"exit 0") log "campaign FINISHED cleanly" ;;
            *) log "campaign FAILED ($outcome)" ;;
            esac
        else
            log "no campaign session and no completion marker — never started, or killed"
        fi
        log "last lines of $LOG:"
        rsh "$LOADGEN_ADDR" "tail -20 $LOG 2>/dev/null" || log "(no log yet)"
    fi
    ;;

logs)
    load_rig
    if [ "${1:-}" = "-f" ]; then
        rsh "$LOADGEN_ADDR" "tail -f $LOG"
    else
        rsh "$LOADGEN_ADDR" "cat $LOG"
    fi
    ;;

attach)
    load_rig
    # -t so tmux gets a terminal; this is the one interactive path.
    ssh -t "${SSH_OPTS[@]}" "$LOADGEN_ADDR" "tmux attach -t $SESSION"
    ;;

fetch)
    # Results also go to S3 per cell; this is the convenience path.
    load_rig
    mkdir -p "$RESULTS_ROOT"
    log "syncing s3://$BUCKET/campaigns/ -> $RESULTS_ROOT/s3-mirror/"
    aws s3 sync --region "$REGION" "s3://$BUCKET/campaigns/" "$RESULTS_ROOT/s3-mirror/"
    log "fetched into $RESULTS_ROOT/s3-mirror/"
    ;;

stop)
    load_rig
    rsh "$LOADGEN_ADDR" "tmux kill-session -t $SESSION 2>/dev/null" || true
    log "campaign session killed"
    # run-campaign.sh releases the fleet on SIGHUP, but tmux may SIGKILL
    # before that finishes. Scale to zero here too — it is idempotent,
    # and leaving 20 spot instances up is the failure that costs money.
    scale_workers 0 || log "WARNING: could not scale the fleet to 0 — check the ASG"
    log "spot fleet released; the control plane is still up (teardown.sh destroys it)"
    ;;

help | -h | --help) usage 0 ;;
*)
    echo "unknown action: $ACTION" >&2
    usage 1
    ;;
esac
