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
    # `cloud-init status --wait` exits non-zero when the run ended in
    # error, and under `set -e` that killed bringup with no output at
    # all — the box, the failing module, and the reason were all on the
    # instance and never surfaced. Report them here instead: a cloud-init
    # failure is a first-boot bug that has to be read to be fixed.
    if ! rsh "$ip" "cloud-init status --wait >/dev/null"; then
        echo "cloud-init failed on $ip:" >&2
        rsh "$ip" "cloud-init status --long 2>&1 | sed 's/^/  /'" >&2 || true
        rsh "$ip" "sudo tail -30 /var/log/cloud-init-output.log 2>&1 | sed 's/^/  /'" >&2 || true
        exit 1
    fi
    # node_exporter listens on 9100, which security.tf opens ONLY
    # intra-rig (the `self` rule) — deliberately not to admin_cidr. So a
    # direct poll can succeed on-rig and can never succeed off-rig, where
    # it burns its full timeout on every box before the ssh fallback does
    # the real check. That was ~8 minutes of guaranteed-useless waiting
    # per bringup. Branch on where we are instead.
    if [ "$BENCH_ON_RIG" = 1 ]; then
        wait_for_http "http://$ip:9100/metrics" 120
    else
        rsh "$ip" 'for _ in $(seq 1 24); do
                       curl -fsS -m 3 http://localhost:9100/metrics >/dev/null 2>&1 && exit 0
                       sleep 5
                   done
                   exit 1' || {
            echo "node_exporter never came up on $ip" >&2
            exit 1
        }
    fi
done

"$(dirname "$0")/install-loadgen.sh"

log "rig is up:"
terraform -chdir="$TF_DIR" output ssh_config
log "next: build-and-upload.sh, then install-legion-a.sh (or -b/nomad/k8s)"
