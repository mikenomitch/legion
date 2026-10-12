#!/usr/bin/env bash
# Runs a legion CLI command on one manager, against that manager itself, with
# the management token. Use it for commands that must run on a specific
# member, like making it the raft leader:
#
#   tests/spot-check/infra/on-manager.sh sc-mgr-1 raft command trigger-election
#   tests/spot-check/infra/on-manager.sh sc-mgr-2 raft status
set -euo pipefail
source "$(dirname "$0")/env.sh"
name="${1:?usage: on-manager.sh <sc-mgr-N> <legion args...>}"
shift
printf -v args '%q ' "$@"
sc_ssh "$name" "LEGION_ADDR=http://127.0.0.1:4747 LEGION_TOKEN=$(cat "$SC_STATE/token") legion $args"
