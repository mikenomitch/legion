#!/usr/bin/env bash
# Stand the rig up: terraform apply, wait for every box, verify
# node_exporter, then install the loadgen observability stack. Safe to
# re-run — everything downstream is idempotent.
#
# Usage: bringup.sh [terraform apply args, e.g. -var worker_instances=4]
set -euo pipefail
source "$(dirname "$0")/env.sh"

log "terraform apply"
terraform -chdir="$TF_DIR" init -upgrade=false -input=false >/dev/null
terraform -chdir="$TF_DIR" apply -auto-approve -input=false "$@"

load_rig

log "waiting for SSH on the control plane + loadgen"
for ip in "${CP_ADDRS[@]}" "$LOADGEN_ADDR"; do
    wait_for_ssh "$ip"
done

log "waiting for cloud-init (node_exporter) on every box"
for ip in "${CP_ADDRS[@]}" "$LOADGEN_ADDR"; do
    # A cloud-init failure is reported, not fatal. It was fatal, and that
    # turned an observability-only package problem (docker-compose-v2, no
    # longer resolvable on Noble) into a dead campaign -- twice -- even
    # though node_exporter had installed fine. The probe immediately below
    # is the actual requirement, so let it decide.
    if ! rsh "$ip" "cloud-init status --wait >/dev/null"; then
        echo "WARNING: cloud-init reported a failure on $ip. The node_exporter" >&2
        echo "         probe below decides whether it matters:" >&2
        rsh "$ip" "cloud-init status --long 2>&1 | sed 's/^/  /'" >&2 || true
        rsh "$ip" "sudo tail -10 /var/log/cloud-init-output.log 2>&1 | sed 's/^/  /'" >&2 || true
    fi
    # node_exporter listens on 9100, which security.tf opens ONLY
    # intra-rig — so probing it from an operator box could never succeed
    # and used to burn a full 120s timeout per box. rig_http_ready picks
    # the addressing that can actually work here.
    rig_http_ready "$ip" 9100/metrics 120 || {
        echo "node_exporter never came up on $ip" >&2
        exit 1
    }
done

"$(dirname "$0")/install-loadgen.sh"

log "rig is up:"
terraform -chdir="$TF_DIR" output ssh_config
log "next: build-and-upload.sh, then install-legion-a.sh (or -b/nomad/k8s)"
