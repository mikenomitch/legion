#!/usr/bin/env bash
# Makes one benchmark job the only one running (each one's envelope takes most
# of a node), and waits until every worker accepts its sessions.
#
#   benchmarks/aws/sandbox-boot/use-job.sh jobs/sessions-vm.ncl
set -euo pipefail
source "$(dirname "$0")/env.sh"
spec="$1"
job="$(sed -n 's/^ *name = "\([^"]*\)",.*/\1/p' "$spec" | head -1)"
task="$(sed -n 's/^ *name = "\([^"]*\)",.*/\1/p' "$spec" | sed -n 2p)"
for other in $("$SB_DIR/cli.sh" job list -j | jq -r '.data[] | select(.status == "running" or .status == "pending") | .name' 2>/dev/null); do
    [ "$other" = "$job" ] && continue
    "$SB_DIR/clear.sh" "$other" >/dev/null 2>&1 || true
    "$SB_DIR/cli.sh" job stop "$other" >/dev/null 2>&1 || true
done
"$SB_DIR/cli.sh" job run "$spec" >/dev/null
for _ in $(seq 60); do
    accepting="$("$SB_DIR/cli.sh" session nodes "$job" --task "$task" -j 2>/dev/null | jq '[.data.nodes[] | select(.accepting)] | length')"
    [ "${accepting:-0}" -ge "${#SB_WORKERS[@]}" ] && { echo "$job/$task accepting on $accepting node(s)"; exit 0; }
    sleep 2
done
echo "$job/$task: only ${accepting:-0} of ${#SB_WORKERS[@]} node(s) accepting" >&2
exit 1
