#!/usr/bin/env bash
# SSH to a rig box by name, optionally running a command.
#
#   tests/spot-check/infra/ssh.sh sc-wkr-1
#   tests/spot-check/infra/ssh.sh sc-mgr-1 journalctl -u legion-manager -n 50
set -euo pipefail
source "$(dirname "$0")/env.sh"
name="${1:?usage: ssh.sh <sc-mgr-N|sc-wkr-N> [command...]}"
shift
sc_ssh "$name" -t "$@"
