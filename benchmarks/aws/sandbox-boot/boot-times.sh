#!/usr/bin/env bash
# Prints the per-session start timeline (boot-times.py) for each worker.
#
#   benchmarks/aws/sandbox-boot/boot-times.sh '2 min ago'
set -euo pipefail
source "$(dirname "$0")/env.sh"
since="${1:-10 min ago}"
for name in "${SB_WORKERS[@]}"; do
    sb_scp_to "$name" "$SB_DIR/boot-times.py" /tmp/boot-times.py
    echo "== $name"
    sb_ssh "$name" "sudo python3 /tmp/boot-times.py --since '$since'"
done
