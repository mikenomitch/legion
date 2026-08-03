#!/usr/bin/env bash
# Failover under load: kill the control-plane leader/primary while a
# trickle of jobs is flowing; measure how long placements stop
# (storm.placement_gap_secs in the driver output).
#
# Usage: failover-drill.sh --system legion-a|legion-b|nomad|k8s [--campaign <id>]
set -euo pipefail
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../scripts" && pwd)"
source "$SCRIPTS/env.sh"
load_rig

SYSTEM="" CAMPAIGN=""
while [ $# -gt 0 ]; do
    case "$1" in
    --system) SYSTEM="$2" ;;
    --campaign) CAMPAIGN="$2" ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
    shift 2
done
: "${SYSTEM:?}"
CAMPAIGN="${CAMPAIGN:-adhoc-$(date -u +%Y%m%d-%H%M%S)}"

# Find the victim: raft leader (legion-a), lease primary (legion-b),
# nomad leader, or the scheduler lease holder (k8s).
case "$SYSTEM" in
legion-a)
    DRIVER_SYSTEM=legion MANAGER="http://${CP_PRIVATE_IPS[0]}:4747"
    VICTIM=""
    for idx in 0 1 2; do
        if rsh "${CP_ADDRS[$idx]}" "legion raft status 2>/dev/null" | grep -qi "role.*primary\|leader.*self\|is_leader.*true"; then
            VICTIM="${CP_PRIVATE_IPS[$idx]}"
            break
        fi
    done
    # Fallback: readyz requires colocated lane leadership, so a ready box
    # is the leader under the lanes topology.
    [ -n "$VICTIM" ] || for idx in 0 1 2; do
        if curl -fsS -m 3 "http://${CP_ADDRS[$idx]}:4749/readyz" >/dev/null 2>&1; then
            VICTIM="${CP_PRIVATE_IPS[$idx]}"
            break
        fi
    done
    FAULT_CMD="$RSH_FROM_LOADGEN ubuntu@$VICTIM sudo systemctl stop legion-manager"
    ;;
legion-b)
    DRIVER_SYSTEM=legion
    VICTIM=""
    for idx in 0 1 2; do
        if rsh "$LOADGEN_ADDR" "curl -fsS -m 3 http://${CP_PRIVATE_IPS[$idx]}:4749/readyz >/dev/null 2>&1"; then
            VICTIM="${CP_PRIVATE_IPS[$idx]}"
            break
        fi
    done
    # Submit via a standby-turned-primary after failover: point the
    # driver at a NON-victim box... placements resume when the lease
    # moves, which is what we measure.
    for ip in "${CP_PRIVATE_IPS[@]}"; do
        [ "$ip" != "$VICTIM" ] && MANAGER="http://$ip:4747" && break
    done
    FAULT_CMD="$RSH_FROM_LOADGEN ubuntu@$VICTIM sudo systemctl stop legion-manager"
    ;;
nomad)
    DRIVER_SYSTEM=nomad MANAGER="http://${CP_PRIVATE_IPS[0]}:4646"
    LEADER=$(rsh "${CP_ADDRS[0]}" "nomad operator api /v1/status/leader" | tr -d '"' | cut -d: -f1)
    FAULT_CMD="$RSH_FROM_LOADGEN ubuntu@$LEADER sudo systemctl stop nomad"
    VICTIM="$LEADER"
    ;;
k8s)
    DRIVER_SYSTEM=k8s MANAGER=""
    # The active scheduler holds the kube-system lease; kill its host's
    # static-pod kubelet won't do — move the scheduler manifest away.
    HOLDER=$(rsh "$LOADGEN_ADDR" "kubectl get lease -n kube-system kube-scheduler -o jsonpath='{.spec.holderIdentity}'" | cut -d_ -f1)
    VICTIM=$(rsh "$LOADGEN_ADDR" "kubectl get node $HOLDER -o jsonpath='{.status.addresses[?(@.type==\"InternalIP\")].address}'")
    FAULT_CMD="$RSH_FROM_LOADGEN ubuntu@$VICTIM sudo mv /etc/kubernetes/manifests/kube-scheduler.yaml /tmp/"
    ;;
esac
[ -n "${VICTIM:-}" ] || {
    echo "could not locate the $SYSTEM leader/primary" >&2
    exit 1
}
log "failover drill: victim $VICTIM"

RUN_ID="experiments/failover-$(date -u +%Y%m%d-%H%M%S)"
REMOTE_OUT="/opt/bench/runs/$CAMPAIGN/$RUN_ID"

# Trickle submitter: one small job every 2s keeps placements flowing so
# the gap is observable. Runs on the loadgen box for the whole drill.
rsh "$LOADGEN_ADDR" "
    set -euo pipefail
    mkdir -p '$REMOTE_OUT'
    python3 /opt/bench/workloads/translate.py \
        --scenario /opt/bench/scenarios/steady_small.json \
        --system '$DRIVER_SYSTEM' --count-multiplier 5 \
        --out '$REMOTE_OUT/trickle' >/dev/null 2>&1 || true
    (
      i=0
      for f in '$REMOTE_OUT'/trickle/*.ncl '$REMOTE_OUT'/trickle/*.hcl '$REMOTE_OUT'/trickle/*.json; do
        [ -e \"\$f\" ] || continue
        case \"\$f\" in
          *manifest.json) continue ;;
          *.ncl) LEGION_ADDR='$MANAGER' legion job run \"\$f\" -j >/dev/null 2>&1 || true ;;
          *.hcl) NOMAD_ADDR='$MANAGER' nomad job run -detach \"\$f\" >/dev/null 2>&1 || true ;;
          *.json) kubectl apply -f \"\$f\" >/dev/null 2>&1 || true ;;
        esac
        i=\$((i+1)); sleep 2
      done
    ) &
    TRICKLE_PID=\$!
    python3 /opt/bench/driver/driver.py watch \
        --system '$DRIVER_SYSTEM' \
        ${MANAGER:+--manager '$MANAGER'} \
        --out '$REMOTE_OUT/run' \
        --exec-after-secs 20 \
        --exec-cmd '$FAULT_CMD' \
        --settle-timeout-secs 180 \
        --legion-bin /usr/local/bin/legion \
        | tee '$REMOTE_OUT/metrics.json'
    kill \$TRICKLE_PID 2>/dev/null || true
"

# Restore the victim so the environment is whole for the next cell.
case "$SYSTEM" in
legion-*) rsh_target=$(for idx in 0 1 2; do [ "${CP_PRIVATE_IPS[$idx]}" = "$VICTIM" ] && echo "${CP_ADDRS[$idx]}"; done)
    rsh "$rsh_target" "sudo systemctl start legion-manager" ;;
nomad) rsh_target=$(for idx in 0 1 2; do [ "${CP_PRIVATE_IPS[$idx]}" = "$VICTIM" ] && echo "${CP_ADDRS[$idx]}"; done)
    rsh "$rsh_target" "sudo systemctl start nomad" ;;
k8s) rsh_target=$(for idx in 0 1 2; do [ "${CP_PRIVATE_IPS[$idx]}" = "$VICTIM" ] && echo "${CP_ADDRS[$idx]}"; done)
    rsh "$rsh_target" "sudo mv /tmp/kube-scheduler.yaml /etc/kubernetes/manifests/" ;;
esac

"$SCRIPTS/collect.sh" --campaign "$CAMPAIGN" --run "$RUN_ID" --system "$SYSTEM" \
    --nodes 0 --scenario failover
log "drill complete: placement_gap_secs in $RESULTS_ROOT/$CAMPAIGN/$RUN_ID/metrics.json"
