#!/usr/bin/env bash
# Return the control-plane boxes to a clean slate between campaigns:
# stop and remove every system under test, delete its state. The boxes
# themselves are never recreated — same hardware, serially, is the
# comparison's credibility argument.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

for idx in "${!CP_ADDRS[@]}"; do
    ip="${CP_ADDRS[$idx]}"
    log "wiping cp-$idx ($ip)"
    rsh "$ip" "
        sudo systemctl disable --now legion-manager 2>/dev/null || true
        sudo systemctl disable --now nomad 2>/dev/null || true
        if command -v kubeadm >/dev/null 2>&1; then sudo kubeadm reset -f 2>/dev/null || true; fi
        sudo rm -rf /var/lib/legion /var/lib/nomad /etc/kubernetes /var/lib/etcd
        sudo rm -f /etc/systemd/system/legion-manager.service /etc/systemd/system/nomad.service
        sudo systemctl daemon-reload
    "
done
log "control plane wiped"
