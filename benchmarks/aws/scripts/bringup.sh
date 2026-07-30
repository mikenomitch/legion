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
for ip in "${CP_PUBLIC_IPS[@]}" "$LOADGEN_PUBLIC_IP"; do
    wait_for_ssh "$ip"
done

log "waiting for cloud-init (node_exporter) on every box"
for ip in "${CP_PUBLIC_IPS[@]}" "$LOADGEN_PUBLIC_IP"; do
    rsh "$ip" "cloud-init status --wait >/dev/null"
    wait_for_http "http://$ip:9100/metrics" 120 2>/dev/null ||
        rsh "$ip" "curl -fsS -m 3 http://localhost:9100/metrics >/dev/null"
done

"$(dirname "$0")/install-loadgen.sh"

log "rig is up:"
terraform -chdir="$TF_DIR" output ssh_config
log "next: build-and-upload.sh, then install-legion-a.sh (or -b/nomad/k8s)"
