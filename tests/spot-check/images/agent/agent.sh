#!/bin/sh
# A stand-in for a coding agent: it "works" on AGENT_TASK in /workspace,
# appending to a journal that should survive restarts (the volume), and logs
# what it does so `legion job logs` and the launcher have something to show.
set -u
ws="${LEGION_STORAGE_DIR:-/workspace}"
mkdir -p "$ws"
cd "$ws" || exit 1

[ -d .git ] || git init -q -b main . 2>/dev/null || git init -q .
boots=$(( $(cat .boots 2>/dev/null || echo 0) + 1 ))
echo "$boots" >.boots

echo "agent ${LEGION_INSTANCE_ID:-local} up (boot #$boots) on $(hostname), task: ${AGENT_TASK:-explore}"
step=0
while true; do
    step=$((step + 1))
    line="$(date -u +%H:%M:%S) step $step: thinking about ${AGENT_TASK:-explore}"
    echo "$line" >>journal.md
    echo "$line"
    sleep 15
done
