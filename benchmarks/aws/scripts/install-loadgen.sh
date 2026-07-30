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
cp "$REPO_ROOT"/benchmarks/scheduler/scenarios/*.json "$STAGE/scenarios/"

log "syncing to loadgen ($LOADGEN_PUBLIC_IP)"
rsh "$LOADGEN_PUBLIC_IP" "sudo mkdir -p /opt/bench && sudo chown ubuntu:ubuntu /opt/bench"
tar -C "$STAGE" -czf - . | rsh "$LOADGEN_PUBLIC_IP" "tar -C /opt/bench -xzf -"

log "installing legion CLI from s3://$BUCKET/bin/legion"
rsh "$LOADGEN_PUBLIC_IP" "
    sudo aws s3 cp --region $REGION s3://$BUCKET/bin/legion /opt/legion/bin/legion &&
    sudo chmod +x /opt/legion/bin/legion &&
    sudo ln -sf /opt/legion/bin/legion /usr/local/bin/legion
" || log "WARNING: legion binary not in S3 yet — run build-and-upload.sh, then re-run"

"$(dirname "$0")/gen-targets.sh"

log "starting Prometheus + Grafana"
rsh "$LOADGEN_PUBLIC_IP" "cd /opt/bench/observability && sudo docker compose up -d"
wait_for_http "http://$LOADGEN_PUBLIC_IP:9090/-/ready" 120
log "Grafana: http://$LOADGEN_PUBLIC_IP:3000  Prometheus: http://$LOADGEN_PUBLIC_IP:9090"
