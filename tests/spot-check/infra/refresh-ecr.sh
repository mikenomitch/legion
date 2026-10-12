#!/usr/bin/env bash
# Stores a fresh ECR pull credential in Legion KV at secrets/ecr-pull, where
# the managers' artifacts.auth matcher points every oci://<ecr>/** image.
# ECR tokens last 12 hours, so re-run this when private pulls start failing
# with 401s. Rotation needs no job changes: the worker reads the credential at
# fetch time.
#
#   tests/spot-check/infra/refresh-ecr.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"
eval "$("$SC_DIR/infra/cli-env.sh")"

password="$(aws ecr get-login-password)"
value="$(jq -cn --arg p "$password" '{type: "basic", username: "AWS", password: $p}')"
legion kv put secrets/ecr-pull "$value" -t json >/dev/null
sc_log "secrets/ecr-pull refreshed (valid ~12h)"
