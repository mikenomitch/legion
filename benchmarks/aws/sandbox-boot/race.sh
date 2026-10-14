#!/usr/bin/env bash
# Races edge starts of the same named instance on every worker at once and
# checks that ratification lets exactly one keep each instance.
#
#   benchmarks/aws/sandbox-boot/race.sh [IDS] [PARALLEL]   # default 200 ids, 20 at a time
#
# Runs the CLI on the first manager. Each id gets one `legion sandbox start`
# per worker, all fired together. Afterwards it counts, per id, the starts
# that ratified, and, per node, the instances' processes still running.
set -euo pipefail
source "$(dirname "$0")/env.sh"
ids="${1:-200}" parallel="${2:-20}" tag="${RACE_TAG:-race-$(date +%s)}"
nodes=""
for name in "${SB_WORKERS[@]}"; do nodes+="$name=https://$(sb_private_ip "$name"):4848 "; done
sb_scp_to "${SB_MANAGERS[0]}" "$SB_DIR/jobs/race-exec.ncl" /tmp/race-exec.ncl
# A node redeems tickets only for a config it has run (docs/edge-boot.md, "The
# spec cache"), so ordinary instances go first until every worker runs one.
running() { sb_ssh "$1" "pgrep -fc '^/bin/sleep 3600' || true"; }
sb_ssh "${SB_MANAGERS[0]}" "LEGION_ADDR=http://127.0.0.1:4747 LEGION_TOKEN=$(cat "$SB_STATE/token") legion job run /tmp/race-exec.ncl >/dev/null"
for k in $(seq 30); do
    missing=0
    for name in "${SB_WORKERS[@]}"; do [ "$(running "$name")" -gt 0 ] || missing=1; done
    [ "$missing" = 0 ] && break
    "$SB_DIR/cli.sh" instance create race --id "warm-$tag-$k" >/dev/null 2>&1 || true
    sleep 3
done
before=()
for name in "${SB_WORKERS[@]}"; do before+=("$(running "$name")"); done
sb_ssh "${SB_MANAGERS[0]}" "export LEGION_ADDR=http://127.0.0.1:4747 LEGION_TOKEN=$(cat "$SB_STATE/token")
    legion net ca show > /tmp/ca.pem
    T=\$(legion boot-ticket create race --task box --id-prefix $tag- --pool default --gate serve --uses 100000 --ttl 1h -j | jq -r .data.ticket)
    [ -n \"\$T\" ] && [ \"\$T\" != null ] || { echo 'could not mint the boot ticket' >&2; exit 1; }
    mkdir -p /tmp/$tag
    race_one() {
        for pair in $nodes; do
            name=\${pair%%=*} addr=\${pair#*=}
            legion sandbox start race --id $tag-\$1 --node \$addr --node-name \$name --ca /tmp/ca.pem --ticket \"\$T\" > /tmp/$tag/\$1.\$name 2>&1 &
        done
        wait
    }
    export -f race_one; export T
    seq $ids | xargs -P $parallel -I{} bash -c 'race_one {}'
    cd /tmp/$tag
    won=0; lost=0; other=0; dup=0
    for i in \$(seq $ids); do
        n=\$(grep -l '^ratified' \$i.* 2>/dev/null | wc -l)
        [ \$n -eq 1 ] && won=\$((won+1))
        [ \$n -gt 1 ] && dup=\$((dup+1))
        [ \$n -eq 0 ] && { other=\$((other+1)); echo \"id \$i: no start ratified: \$(cat \$i.* | tr '\n' ' ' | cut -c1-300)\"; }
    done
    echo \"ids=$ids one_winner=\$won two_or_more_winners=\$dup no_winner=\$other\"
    echo \"refusals: \$(cat * | grep -oE 'instance_claimed|already (claimed|owns|bound)[a-z ]*|refused[^\"]*' | sort | uniq -c | sort -rn | head -5 | tr '\n' ';')\""
sleep 5
total=0
for i in "${!SB_WORKERS[@]}"; do
    name="${SB_WORKERS[$i]}"
    n=$(($(running "$name") - before[i]))
    echo "$name runs $n raced instances"
    total=$((total + n))
done
echo "processes across nodes: $total (expect one per id still running)"
