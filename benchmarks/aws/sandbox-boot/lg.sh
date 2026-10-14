#!/usr/bin/env bash
# Runs legion-loadgen on a rig box (default: the first manager) against the
# managers, with the rig's management token. Arguments pass through.
#
#   benchmarks/aws/sandbox-boot/lg.sh --mode sessions --job bench-vm --task vm --count 40
#   SB_FROM=sbx-lg-1 benchmarks/aws/sandbox-boot/lg.sh ...
set -euo pipefail
source "$(dirname "$0")/env.sh"
from="${SB_FROM:-${SB_MANAGERS[0]}}"
managers=""
for name in "${SB_MANAGERS[@]}"; do managers+="http://$(sb_private_ip "$name"):4747,"; done
args=""
for arg in "$@"; do args+=" $(printf '%q' "$arg")"; done
sb_ssh "$from" "LEGION_TOKEN=$(cat "$SB_STATE/token") legion-loadgen --manager ${managers%,} --quiet$args"
