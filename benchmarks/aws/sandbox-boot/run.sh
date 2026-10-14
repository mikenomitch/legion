#!/usr/bin/env bash
# One measured run: clear the job's sessions, start COUNT sessions with the
# loadgen, wait for the guests, and merge the loadgen's summary with every
# worker's boot timeline (boot-times.py) into .state/results/NAME.json.
#
#   benchmarks/aws/sandbox-boot/run.sh NAME JOB TASK [loadgen args...]
#   benchmarks/aws/sandbox-boot/run.sh base-burst40 bench-vm vm --count 40 --concurrency 8 --wait running
#
# After the last start it polls the workers' timelines until they cover every
# started session (journald lags behind a CPU-saturated node), for up to
# SB_SETTLE seconds (default 90). Timestamps come from the first worker's
# clock, never the laptop's.
set -euo pipefail
source "$(dirname "$0")/env.sh"
name="$1" job="$2" task="$3"
shift 3
out_dir="$SB_STATE/results"
mkdir -p "$out_dir"

"$SB_DIR/clear.sh" "$job" >/dev/null
since="$(sb_ssh "${SB_WORKERS[0]}" "date -u '+%Y-%m-%d %H:%M:%S'")"
summary="$("$SB_DIR/lg.sh" --mode sessions --job "$job" --task "$task" "$@")"
started="$(jq '.started' <<<"$summary")"
for worker in "${SB_WORKERS[@]}"; do sb_scp_to "$worker" "$SB_DIR/boot-times.py" /tmp/boot-times.py; done
deadline=$(($(date +%s) + ${SB_SETTLE:-90}))
while :; do
    rows="[]"
    for worker in "${SB_WORKERS[@]}"; do
        node_rows="$(sb_ssh "$worker" "sudo python3 /tmp/boot-times.py --since '$since' --json" | jq --arg n "$worker" '[.rows[] | . + {node: $n}]')"
        rows="$(jq -s 'add' <(echo "$rows") <(echo "$node_rows"))"
    done
    [ "$(jq length <<<"$rows")" -ge "$started" ] && break
    [ "$(date +%s)" -ge "$deadline" ] && { echo "timelines cover $(jq length <<<"$rows") of $started sessions" >&2; break; }
    sleep 3
done

jq -n --arg name "$name" --argjson loadgen "$summary" --argjson rows "$rows" '
  def pct(p): sort | if length == 0 then null else .[((length - 1) * p | round)] end;
  {
    name: $name,
    loadgen: $loadgen,
    guests: {
      sessions: ($rows | length),
      to_vmm_ms: { p50: ($rows | map(.to_vmm_ms) | pct(0.5)), p90: ($rows | map(.to_vmm_ms) | pct(0.9)), p99: ($rows | map(.to_vmm_ms) | pct(0.99)), max: ($rows | map(.to_vmm_ms) | max) },
      to_up_ms: { p50: ($rows | map(.to_up_ms) | pct(0.5)), p90: ($rows | map(.to_up_ms) | pct(0.9)), p99: ($rows | map(.to_up_ms) | pct(0.99)), max: ($rows | map(.to_up_ms) | max) },
      window_s: (if ($rows | length) > 0 then (($rows | map(.prepare_unix + .to_up_ms / 1000) | max) - ($rows | map(.prepare_unix) | min)) else null end)
    },
    rows: $rows
  } | .guests.guests_up_per_sec = (if .guests.window_s then (.guests.sessions / .guests.window_s) else null end)
' >"$out_dir/$name.json"
jq -c '{name, started: .loadgen.started, refused: .loadgen.refused, failed: .loadgen.failed, accept_per_s: .loadgen.starts_per_sec, accept_ms: .loadgen.latency_ms, guests: .guests}' "$out_dir/$name.json"
