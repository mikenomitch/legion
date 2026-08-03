#!/usr/bin/env bash
# Legion B (streaming persistence) durability drill: measure the
# lease-failover gap and the RPO when the primary dies mid-write.
#
# A writer stamps sequenced KV keys at WRITE_HZ against the primary;
# we kill the primary process; after the lease moves we read back the
# highest surviving sequence from the new primary.
#
#   RPO_secs  = (last_written_seq - last_survived_seq) / WRITE_HZ
#   gap_secs  = first successful write after the kill - kill time
#
# Publish alongside syncInterval (1s in manager-b.ncl.tpl) and the
# lease TTL (15s) — the numbers are functions of both.
#
# Usage: streaming-rpo.sh [--write-hz 10] [--campaign <id>]
set -euo pipefail
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../scripts" && pwd)"
source "$SCRIPTS/env.sh"
load_rig

WRITE_HZ=10 CAMPAIGN=""
while [ $# -gt 0 ]; do
    case "$1" in
    --write-hz) WRITE_HZ="$2" ;;
    --campaign) CAMPAIGN="$2" ;;
    *)
        echo "unknown arg $1" >&2
        exit 1
        ;;
    esac
    shift 2
done
CAMPAIGN="${CAMPAIGN:-adhoc-$(date -u +%Y%m%d-%H%M%S)}"

# Locate the current primary (the ready box).
PRIMARY_IDX=""
for idx in 0 1 2; do
    if curl -fsS -m 3 "http://${CP_ADDRS[$idx]}:4749/readyz" >/dev/null 2>&1; then
        PRIMARY_IDX=$idx
        break
    fi
done
[ -n "$PRIMARY_IDX" ] || {
    echo "no streaming primary found — is Legion B installed?" >&2
    exit 1
}
PRIMARY_PRIVATE="${CP_PRIVATE_IPS[$PRIMARY_IDX]}"
PRIMARY_PUBLIC="${CP_ADDRS[$PRIMARY_IDX]}"
log "streaming primary: cp-$PRIMARY_IDX ($PRIMARY_PRIVATE), writing at ${WRITE_HZ}Hz"

RUN_ID="experiments/rpo-$(date -u +%Y%m%d-%H%M%S)"
REMOTE_OUT="/opt/bench/runs/$CAMPAIGN/$RUN_ID"

rsh "$LOADGEN_ADDR" "
    set -euo pipefail
    mkdir -p '$REMOTE_OUT'
    export LEGION_ADDR='http://$PRIMARY_PRIVATE:4747'
    INTERVAL=\$(python3 -c 'print(1.0/$WRITE_HZ)')

    # Writer: sequenced keys until killed; records every acked seq.
    (
      seq_num=0
      while :; do
        if legion kv put bench/rpo/seq \"\$seq_num\" >/dev/null 2>&1; then
          echo \"\$seq_num \$(date +%s%3N)\" >> '$REMOTE_OUT/acked.log'
        fi
        seq_num=\$((seq_num+1))
        sleep \"\$INTERVAL\"
      done
    ) &
    WRITER_PID=\$!
    sleep 20   # steady writes before the fault

    KILL_MS=\$(date +%s%3N)
    echo \"\$KILL_MS\" > '$REMOTE_OUT/kill_ms'
    $RSH_FROM_LOADGEN ubuntu@$PRIMARY_PRIVATE sudo systemctl stop legion-manager

    # Wait for a standby to take the lease (TTL 15s + renew), then find it.
    NEW_PRIMARY=''
    for _ in \$(seq 1 60); do
      for ip in ${CP_PRIVATE_IPS[*]}; do
        [ \"\$ip\" = \"$PRIMARY_PRIVATE\" ] && continue
        if curl -fsS -m 2 \"http://\$ip:4749/readyz\" >/dev/null 2>&1; then NEW_PRIMARY=\$ip; break 2; fi
      done
      sleep 2
    done
    [ -n \"\$NEW_PRIMARY\" ] || { echo 'no failover within 120s' >&2; kill \$WRITER_PID; exit 1; }
    echo \"\$NEW_PRIMARY\" > '$REMOTE_OUT/new_primary'

    # Gap: first successful write against the new primary.
    export LEGION_ADDR=\"http://\$NEW_PRIMARY:4747\"
    until legion kv put bench/rpo/post-failover ok >/dev/null 2>&1; do sleep 0.2; done
    RECOVER_MS=\$(date +%s%3N)

    kill \$WRITER_PID 2>/dev/null || true
    SURVIVED=\$(legion kv get bench/rpo/seq -j 2>/dev/null | jq -r '.data.value // .data // \"-1\"')
    LAST_WRITTEN=\$(tail -1 '$REMOTE_OUT/acked.log' | cut -d' ' -f1)

    python3 - \"\$SURVIVED\" \"\$LAST_WRITTEN\" \"\$KILL_MS\" \"\$RECOVER_MS\" <<'PY' | tee '$REMOTE_OUT/metrics.json'
import json, sys
survived, last_written, kill_ms, recover_ms = sys.argv[1:5]
survived, last_written = int(float(survived)), int(last_written)
print(json.dumps({
    "write_hz": $WRITE_HZ,
    "last_written_seq": last_written,
    "last_survived_seq": survived,
    "lost_writes": max(0, last_written - survived),
    "rpo_secs": round(max(0, last_written - survived) / $WRITE_HZ, 3),
    "failover_gap_secs": round((int(recover_ms) - int(kill_ms)) / 1000.0, 3),
}, indent=2))
PY
"

# Restart the old primary as a standby.
rsh "$PRIMARY_PUBLIC" "sudo systemctl start legion-manager"

"$SCRIPTS/collect.sh" --campaign "$CAMPAIGN" --run "$RUN_ID" --system legion-b \
    --nodes 0 --scenario rpo
log "RPO drill complete: $RESULTS_ROOT/$CAMPAIGN/$RUN_ID/metrics.json"
