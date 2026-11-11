#!/usr/bin/env bash
# Runs the legion CLI on the first manager with the rig's management token.
#
#   benchmarks/aws/sandbox-boot/cli.sh job status bench-vm
#   benchmarks/aws/sandbox-boot/cli.sh job run jobs/sessions-vm.ncl   # local paths are copied over
set -euo pipefail
source "$(dirname "$0")/env.sh"
args=()
for arg in "$@"; do
    if [ -f "$arg" ]; then
        remote="/tmp/$(basename "$arg")"
        sb_scp_to "${SB_MANAGERS[0]}" "$arg" "$remote"
        args+=("$remote")
    else
        args+=("$(printf '%q' "$arg")")
    fi
done
sb_ssh "${SB_MANAGERS[0]}" "LEGION_ADDR=http://127.0.0.1:4747 LEGION_TOKEN=$(cat "$SB_STATE/token") legion ${args[*]}"
