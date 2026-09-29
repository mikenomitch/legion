#!/usr/bin/env bash
# Everything a rig instance needs that is IDENTICAL on every box, in one
# place, so it can be baked into an AMI once instead of installed on every
# first boot.
#
# Measured 2026-08-05: this work costs ~10 minutes per rig bring-up and
# 6-8 minutes on every spot-worker scale-up, because it is nearly all
# `apt-get` and downloads against a mirror whose throughput varies by an
# order of magnitude. A campaign that scales the fleet several times pays
# it several times.
#
# What is deliberately NOT here, because it cannot be baked:
#   - the instance-store mount: the NVMe is ephemeral, comes back blank
#     and unformatted after any stop/start, and its device name is not
#     stable, so it must be discovered and formatted at every boot;
#   - the hostname and /opt/bench/role, which are per-instance;
#   - anything that starts the system under test — the campaign scripts
#     install Legion/Nomad/K8s over SSH by design.
#
# Run by bake-rig-ami.sh (into an image) and by cloud-init's common.sh
# (when no baked image is in use). The marker it drops is what lets the
# boot-time path skip work the image already did.
set -euo pipefail

NODE_EXPORTER_VERSION="${NODE_EXPORTER_VERSION:-1.8.2}"
MARKER=/opt/bench/.rig-toolchain-baked

log() { echo "[rig-toolchain] $*"; }

log "installing base packages"
DEBIAN_FRONTEND=noninteractive apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get install -y \
    chrony jq unzip gettext-base curl ca-certificates

# AWS CLI v2 from the bundled installer, NOT `apt-get install awscli`:
# Ubuntu 24.04 (noble) dropped the awscli package from the archive
# entirely, so the apt route fails the whole run with "Package 'awscli'
# has no installation candidate".
if ! command -v aws >/dev/null 2>&1; then
    log "installing AWS CLI v2"
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
    unzip -q -o /tmp/awscliv2.zip -d /tmp
    /tmp/aws/install --update
    rm -rf /tmp/aws /tmp/awscliv2.zip
fi

# node_exporter: pinned release, dedicated user, systemd-managed. Enabled
# but harmless to bake — it starts on boot and exports the same metrics
# whatever role the instance ends up playing.
if [ ! -x /usr/local/bin/node_exporter ]; then
    log "installing node_exporter $NODE_EXPORTER_VERSION"
    useradd --no-create-home --shell /usr/sbin/nologin node_exporter || true
    curl -fsSL -o /tmp/node_exporter.tar.gz \
        "https://github.com/prometheus/node_exporter/releases/download/v${NODE_EXPORTER_VERSION}/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz"
    tar -xzf /tmp/node_exporter.tar.gz -C /tmp
    install -m 0755 "/tmp/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64/node_exporter" \
        /usr/local/bin/node_exporter
    rm -rf /tmp/node_exporter.tar.gz "/tmp/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64"
fi

cat >/etc/systemd/system/node_exporter.service <<'UNIT'
[Unit]
Description=Prometheus node_exporter
After=network-online.target

[Service]
User=node_exporter
ExecStart=/usr/local/bin/node_exporter
Restart=always

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable node_exporter
systemctl start node_exporter || true

# Docker + compose on EVERY rig image, not just the loadgen's. One image
# for all roles is worth more than the ~400 MB it costs: a second image
# would need its own bake, its own staleness check, and a way for
# terraform to pick between them per role.
#
# `docker-compose-v2` stopped resolving on Noble (2026-08-05) and took
# docker.io down with it because apt failed the whole line. Engine first,
# on its own line; compose from apt if it resolves, else the upstream
# binary; and neither is allowed to fail the run.
if ! command -v docker >/dev/null 2>&1; then
    log "installing docker"
    DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io
    systemctl enable docker
    usermod -aG docker ubuntu || true
fi
if ! docker compose version >/dev/null 2>&1; then
    log "installing docker compose plugin"
    DEBIAN_FRONTEND=noninteractive apt-get install -y docker-compose-v2 2>/dev/null || {
        mkdir -p /usr/local/lib/docker/cli-plugins
        curl -fsSL "https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64" \
            -o /usr/local/lib/docker/cli-plugins/docker-compose &&
            chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
    } || log "WARNING: docker compose unavailable; the observability stack will not start"
fi

# Directories the campaign scripts install into.
mkdir -p /opt/legion/bin /opt/legion/configs /var/lib/legion /opt/bench

date -u +%FT%TZ >"$MARKER"
log "done — marker at $MARKER"
