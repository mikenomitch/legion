#!/usr/bin/env bash
# Stops every running session of a job, from the first manager, 32 at a time.
#
#   benchmarks/aws/sandbox-boot/clear.sh bench-vm
set -euo pipefail
source "$(dirname "$0")/env.sh"
job="${1:?usage: clear.sh <job>}"
sb_ssh "${SB_MANAGERS[0]}" "export LEGION_ADDR=http://127.0.0.1:4747 LEGION_TOKEN=$(cat "$SB_STATE/token");
    legion instance list $job -j --session-limit 50000 | jq -r '.data.instances[] | select(.mode == \"session\") | .instanceId' |
    xargs -r -P 32 -n 1 legion instance stop >/dev/null 2>&1;
    legion instance list $job -j --session-limit 0 | jq '.data.sessionCount'"
