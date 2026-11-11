#!/usr/bin/env bash
# SSH to a rig box by name, optionally running a command.
#
#   benchmarks/aws/sandbox-boot/ssh.sh sbx-wkr-1
#   benchmarks/aws/sandbox-boot/ssh.sh sbx-mgr-1 journalctl -u legion-manager -n 50
set -euo pipefail
source "$(dirname "$0")/env.sh"
name="${1:?usage: ssh.sh <box> [command...]}"
shift
sb_ssh "$name" "$@"
