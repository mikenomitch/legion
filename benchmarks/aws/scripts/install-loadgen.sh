#!/usr/bin/env bash
# Provision the loadgen box: observability stack (Prometheus + Grafana),
# the driver + workloads + scenarios, and the legion CLI (for job
# submission and the events follower). Idempotent.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

log "rendering observability config"
mkdir -p "$STAGE/observability/targets"
CP0_IP="${CP_PRIVATE_IPS[0]}" CP1_IP="${CP_PRIVATE_IPS[1]}" CP2_IP="${CP_PRIVATE_IPS[2]}" \
    render_tpl CP0_IP CP1_IP CP2_IP \
    <"$AWS_BENCH_ROOT/configs/prometheus/prometheus.yml.tpl" \
    >"$STAGE/observability/prometheus.yml"
cp "$AWS_BENCH_ROOT/configs/prometheus/docker-compose.yaml" "$STAGE/observability/"
cp -r "$AWS_BENCH_ROOT/configs/prometheus/grafana" "$STAGE/observability/"
cp -r "$AWS_BENCH_ROOT/dashboards" "$STAGE/observability/dashboards"

log "staging driver + workloads + scenarios"
cp -r "$AWS_BENCH_ROOT/driver" "$STAGE/driver"
cp -r "$AWS_BENCH_ROOT/workloads" "$STAGE/workloads"
mkdir -p "$STAGE/scenarios"
cp "$SCENARIOS_DIR"/*.json "$STAGE/scenarios/"

log "syncing to loadgen ($LOADGEN_ADDR)"
rsh "$LOADGEN_ADDR" "sudo mkdir -p /opt/bench && sudo chown ubuntu:ubuntu /opt/bench"
tar -C "$STAGE" -czf - . | rsh "$LOADGEN_ADDR" "tar -C /opt/bench -xzf -"

# tmux hosts unattended campaigns (scripts/remote-campaign.sh); the rig
# key lets this box reach the control plane and workers, which the
# fault-injection drills in experiments/ need even for attended runs.
log "installing tmux + the rig ssh key"
rsh "$LOADGEN_ADDR" "command -v tmux >/dev/null || (sudo apt-get update -qq && sudo apt-get install -y -qq tmux)"
if RIG_KEY=$(resolve_local_ssh_key); then
    rcp "$RIG_KEY" "$LOADGEN_ADDR:$RIG_KEY_ON_LOADGEN"
    rsh "$LOADGEN_ADDR" "chmod 600 $RIG_KEY_ON_LOADGEN"
else
    log "WARNING: no local ssh key found — set BENCH_SSH_KEY and re-run."
    log "         Without it the experiments/ drills cannot inject faults."
fi

log "installing legion CLI from s3://$BUCKET/bin/legion"
rsh "$LOADGEN_ADDR" "
    sudo aws s3 cp --region $REGION s3://$BUCKET/bin/legion /opt/legion/bin/legion &&
    sudo chmod +x /opt/legion/bin/legion &&
    sudo ln -sf /opt/legion/bin/legion /usr/local/bin/legion
" || log "WARNING: legion binary not in S3 yet — run build-and-upload.sh, then re-run"

"$(dirname "$0")/gen-targets.sh"

log "starting Prometheus + Grafana"
rsh "$LOADGEN_ADDR" "cd /opt/bench/observability && sudo docker compose up -d"
wait_for_http "http://$LOADGEN_ADDR:9090/-/ready" 120
# Public IPs here on purpose: these two are for a human's browser, and
# the dashboards are only reachable from admin_cidr.
log "Grafana: http://$LOADGEN_PUBLIC_IP:3000  Prometheus: http://$LOADGEN_PUBLIC_IP:9090"
