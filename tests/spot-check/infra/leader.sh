#!/usr/bin/env bash
# Prints the name of the current raft leader (by each manager's
# legion_raft_is_leader metric), or nothing if no manager claims it.
#
#   tests/spot-check/infra/leader.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"
for name in "${SC_MANAGERS[@]}"; do
    ip="$(sc_public_ip "$name")"
    # Read the metrics fully before grepping: `grep -q` exits on the first
    # match, and under pipefail curl's resulting SIGPIPE fails the pipeline.
    metrics="$(curl -fsS --max-time 2 "http://$ip:4749/metrics" 2>/dev/null || true)"
    if grep -Eq '^legion_raft_is_leader(\{[^}]*\})? 1(\.0)?$' <<<"$metrics"; then
        echo "$name"
        exit 0
    fi
done
