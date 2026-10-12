#!/usr/bin/env bash
# Runs the launcher against the AWS rig's current leader with the management
# token, on http://localhost:${LAUNCHER_PORT:-8089}.
#
#   tests/spot-check/launcher/run-rig.sh
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
eval "$("$SC_DIR/infra/cli-env.sh")"
export LAUNCHER_PORT="${LAUNCHER_PORT:-8089}" LEGION_BIN="$(command -v legion)"
exec python3 "$SC_DIR/launcher/launcher.py"
