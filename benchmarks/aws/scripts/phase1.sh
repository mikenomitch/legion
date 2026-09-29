#!/usr/bin/env bash
# Phase 1 of the plan, start to finish, unattended — the chain that
# ../README.md's runbook spells out by hand, with checkpoints so a
# failure resumes instead of restarting.
#
# Runs ON the operator box (scripts/bootstrap-operator-box.sh launch
# starts it there under tmux), never on your laptop: it is hours long,
# every hop is pinned to the operator box's Elastic IP by admin_cidr,
# and the rig bills the whole time.
#
#   1. local-smoke.sh          prove the measurement loop, no AWS cost
#   2. bringup.sh              VPC, control plane, loadgen, spot ASG, S3
#   3. build-and-upload.sh     dist binary + plugins -> the rig's bucket
#   4. install-legion-a.sh     3-voter raft, lanes topology
#   5. packing-density.sh      retire the 1,250-nodes-per-instance guess
#   6. run-campaign.sh --smoke phase 1's exit criterion
#
# Steps 5 and 6 must not share a control plane. The density ladder climbs
# to 4,000 simulated nodes, and Legion cannot deregister a node, so those
# rows are still in the store when the 1,000-node smoke campaign starts —
# which is exactly how a "1,000-node" cell came to be measured against a
# 4,000-row node table, and reported as though it were not.
# `run-campaign.sh` now wipes and reinstalls the control plane before its
# first step, so this boundary is handled inside step 6 rather than left
# to whoever reads this list. Step 4 stays: the density ladder in step 5
# needs a control plane of its own to register against.
#
# It deliberately stops before teardown. The control plane holds the
# state a post-mortem needs and costs ~$0.75/hr; deciding to destroy it
# is a human's call, and `scripts/teardown.sh` is the only thing that
# should make it.
#
# Usage: phase1.sh [--skip-smoke] [--skip-density] [--from <step-number>]
set -euo pipefail

AWS_BENCH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${PHASE1_STATE_DIR:-$HOME/.legion-phase1}"
mkdir -p "$STATE_DIR"

# rustup puts cargo in ~/.cargo/bin and appends its env to ~/.bashrc —
# which Ubuntu's default .bashrc never reaches, because it returns early
# for non-interactive shells. `ssh host "cmd"` is non-interactive, and
# that is precisely how a campaign gets launched, so steps 1 and 3 would
# otherwise die on `cargo: command not found`. Source it here rather than
# relying on the environment, so this script works however it is invoked.
if [ -f "$HOME/.cargo/env" ]; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi

SKIP_SMOKE=false SKIP_DENSITY=false FROM=1
while [ $# -gt 0 ]; do
    case "$1" in
    --skip-smoke)
        SKIP_SMOKE=true
        shift
        ;;
    --skip-density)
        # Step 5 retires the 1,250-nodes-per-instance guess, which only
        # has to happen once per instance type — skip it when a previous
        # campaign already measured it, or when the fleet sizes in play
        # are ones the guess has already carried.
        SKIP_DENSITY=true
        shift
        ;;
    --from)
        FROM="$2"
        shift 2
        ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
done

log() { echo "[phase1 $(date -u +%FT%TZ)] $*"; }

# Checkpoints are what make this resumable. Every step here is already
# idempotent on its own (terraform converges, the installers wipe and
# reinstall), but re-running bringup + a dist build to retry step 5 costs
# half an hour of wall clock for nothing.
step() { # step <number> <name> <command...>
    local n="$1" name="$2"
    shift 2
    local marker="$STATE_DIR/step-$n.done"
    if [ "$n" -lt "$FROM" ]; then
        log "step $n ($name) — skipped, --from $FROM"
        return 0
    fi
    # `--from N` means "re-run from N", so at or past it the checkpoint is
    # ignored. Otherwise asking to redo a step that already succeeded --
    # the whole reason to pass --from -- would silently do nothing.
    if [ -f "$marker" ] && [ "$FROM" -le 1 ]; then
        log "step $n ($name) — already done ($(cat "$marker")); --from $n to re-run"
        return 0
    fi
    rm -f "$marker"
    log "step $n ($name) — starting"
    "$@"
    date -u +%FT%TZ >"$marker"
    log "step $n ($name) — done"
}

cd "$AWS_BENCH_ROOT"

if [ "$SKIP_SMOKE" = false ]; then
    step 1 local-smoke scripts/local-smoke.sh
else
    log "step 1 (local-smoke) — skipped, --skip-smoke"
fi

step 2 bringup scripts/bringup.sh
step 3 build-and-upload scripts/build-and-upload.sh
step 4 install-legion-a scripts/install-legion-a.sh
if [ "$SKIP_DENSITY" = false ]; then
    step 5 packing-density experiments/packing-density.sh
else
    log "step 5 (packing-density) — skipped, --skip-density"
fi
step 6 smoke-campaign scripts/run-campaign.sh --system legion-a --smoke

log "PHASE 1 COMPLETE"
log ""
log "The rig is still up and still billing (~\$0.75/hr for the control"
log "plane; the spot fleet was released when the campaign exited)."
log ""
log "  results:  benchmarks/aws/results/  and  s3://<bucket>/campaigns/"
log "  density:  the measured nodes-per-instance is in the packing-density"
log "            output above — feed it to run-campaign.sh's sizing"
log "  next:     scripts/run-campaign.sh --system legion-a   (the real one)"
log "  done?     scripts/teardown.sh"
