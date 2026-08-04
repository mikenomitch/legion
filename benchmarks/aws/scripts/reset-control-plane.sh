#!/usr/bin/env bash
# Put the control plane back to an empty store, then reinstall the system
# under test on it. This is the ONLY reliable way to remove node records.
#
#   reset-control-plane.sh --system legion-a
#
# Why this script exists, in one paragraph. Legion has no node
# deregistration: `Store::remove_node` exists but nothing in production
# calls it, and `stop-sim-fleet.sh` only stops the sim PROCESSES — every
# node they registered stays in the store, `lost`, forever. So the node
# table is a high-water mark across everything the control plane has ever
# seen, and a campaign that follows a wider run inherits its orphans: a
# 4000-node packing-density ladder followed by a 1000-node campaign left
# 3000 dead rows, the fleet gate (a `>=` count) waved it through, and
# every cell of that campaign was measured against a scheduler carrying
# four times the nodes the results claim. Wiping and reinstalling is what
# makes the next campaign's first cell honest.
#
# The counter-fix — giving every run a unique node-name prefix — is
# deliberately NOT taken: name REUSE is the only thing bounding the node
# table today, so unique prefixes without deregistration would make every
# single run leak its entire fleet instead of overwriting the last one's.
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
"$(dirname "$0")/stop-sim-fleet.sh" || log "WARNING: sim fleet stop reported an error (continuing)"

# Each installer already runs wipe-control-plane.sh as its first act, so
# reinstalling IS the wipe — kept as one call rather than wipe-then-install
# so the two can never drift apart.
"$(dirname "$0")/$INSTALLER"

log "=== control plane reset: $SYSTEM reinstalled on an empty store ==="
