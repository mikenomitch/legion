#!/usr/bin/env bash
# Validates every spot-check jobspec offline (no manager needed).
#
#   tests/spot-check/validate.sh [path/to/legion]
set -uo pipefail
legion="${1:-legion}"
dir="$(cd "$(dirname "$0")" && pwd)"
status=0
for spec in "$dir"/jobs/*.ncl "$dir"/jobs/mac/*.ncl; do
    [ -e "$spec" ] || continue
    if out="$("$legion" job validate "$spec" 2>&1)"; then
        echo "ok    ${spec#"$dir"/}"
    else
        echo "FAIL  ${spec#"$dir"/}"
        echo "$out" | sed 's/^/      /'
        status=1
    fi
done
exit $status
