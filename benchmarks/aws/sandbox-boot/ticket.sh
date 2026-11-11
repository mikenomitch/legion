#!/usr/bin/env bash
# Mints a one-hour session ticket for a job's session task (the rig's
# managers cap tickets at edgeBoot.maxTicketTtl, 1h by default).
#
#   T=$(benchmarks/aws/sandbox-boot/ticket.sh bench-exec shell)
set -euo pipefail
"$(dirname "$0")/cli.sh" boot-ticket create "$1" --task "$2" --pool default --scope session --ttl 1h -j | jq -r '.data.ticket'
