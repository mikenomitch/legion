#!/usr/bin/env bash
# Prints the env vars that point a local `legion` CLI at the spot-check
# cluster's raft leader, with the management token.
#
#   tests/spot-check/infra/cli-env.sh --fish | source      # fish
#   eval "$(tests/spot-check/infra/cli-env.sh)"            # bash/zsh
#
# Writes go only to the leader (docs/raft-operations.md), so this asks each
# manager's metrics endpoint which one leads.
set -euo pipefail
source "$(dirname "$0")/env.sh"

leader=""
for name in "${SC_MANAGERS[@]}"; do
    ip="$(sc_public_ip "$name")"
    [ -n "$ip" ] || continue
    # Read the metrics fully before grepping: `grep -q` exits on the first
    # match, and under pipefail curl's resulting SIGPIPE fails the pipeline.
    metrics="$(curl -fsS --max-time 3 "http://$ip:4749/metrics" 2>/dev/null || true)"
    if grep -Eq '^legion_raft_is_leader(\{[^}]*\})? 1(\.0)?$' <<<"$metrics"; then
        leader="$ip"
        break
    fi
done
leader="${leader:-$(sc_public_ip "${SC_MANAGERS[0]}")}"
token="$(cat "$SC_STATE/token")"

if [ "${1:-}" = "--fish" ]; then
    echo "set -gx LEGION_ADDR http://$leader:4747;"
    echo "set -gx LEGION_TOKEN $token;"
else
    echo "export LEGION_ADDR=http://$leader:4747"
    echo "export LEGION_TOKEN=$token"
fi
