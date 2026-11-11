#!/usr/bin/env bash
# Put the control plane back to an empty store, then reinstall the system
# under test on it.
#
#   reset-control-plane.sh --system legion-a
#
# stop-sim-fleet.sh now forgets every sim node it stops (`legion node
# forget --force`), so this is no longer the only way to empty the node
# table. It is still the way to start a campaign from a store that holds
# NOTHING from before it: no node rows a skipped or failed forget left
# behind (a 4000-node packing-density ladder followed by a 1000-node
# campaign once left 3000 of them, and every cell measured a scheduler
# carrying four times the nodes the results claim), and no jobs, evals,
# or allocations from the previous run either.
#
# Cost: the installers wipe and reinstall all three boxes, a couple of
# minutes, once per campaign. Cheap next to a campaign of cells that
# cannot be published.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

SYSTEM=""
while [ $# -gt 0 ]; do
    case "$1" in
    --system)
        SYSTEM="$2"
        shift 2
        ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
done
: "${SYSTEM:?--system required (legion-a|legion-b|nomad|k8s)}"

case "$SYSTEM" in
legion-a) INSTALLER=install-legion-a.sh ;;
legion-b) INSTALLER=install-legion-b.sh ;;
nomad) INSTALLER=install-nomad.sh ;;
k8s) INSTALLER=install-k8s.sh ;;
*)
    echo "unknown system $SYSTEM (legion-a|legion-b|nomad|k8s)" >&2
    exit 1
    ;;
esac

# Stop the sim fleet FIRST. Its join tokens and node identities belong to
# the cluster that is about to be destroyed; leaving the units running
# through a wipe just means thousands of workers retrying registration
# against a manager that has never heard of them, and the reinstalled
# cluster picking up half a fleet in an order nobody chose.
log "=== resetting the control plane for $SYSTEM ==="
# SIM_FORGET=0: the wipe below removes the node rows, so forgetting them
# one RPC at a time first is minutes of wasted work.
SIM_FORGET=0 "$(dirname "$0")/stop-sim-fleet.sh" || log "WARNING: sim fleet stop reported an error (continuing)"

# Each installer already runs wipe-control-plane.sh as its first act, so
# reinstalling IS the wipe — kept as one call rather than wipe-then-install
# so the two can never drift apart.
"$(dirname "$0")/$INSTALLER"

log "=== control plane reset: $SYSTEM reinstalled on an empty store ==="
