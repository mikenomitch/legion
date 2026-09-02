#!/usr/bin/env bash
# Install Nomad (3 raft servers) on the control plane for the phase-3
# campaign, and build nomad-nodesim into S3 for the fake fleet.
# UNVALIDATED until phase 3 runs it against real instances — kept in
# lockstep with configs/nomad/*; verify against the pinned versions in
# versions.env before the campaign.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

"$(dirname "$0")/wipe-control-plane.sh"

for idx in 0 1 2; do
    ip="${CP_ADDRS[$idx]}"
    log "installing nomad $NOMAD_VERSION on cp-$idx ($ip)"
    rsh "$ip" "
        set -euo pipefail
        curl -fsSL -o /tmp/nomad.zip \
            https://releases.hashicorp.com/nomad/${NOMAD_VERSION}/nomad_${NOMAD_VERSION}_linux_amd64.zip
        sudo unzip -o -q /tmp/nomad.zip -d /usr/local/bin && rm /tmp/nomad.zip
        sudo mkdir -p /etc/nomad.d /var/lib/nomad
    "
    PRIVATE_IP="${CP_PRIVATE_IPS[$idx]}" \
        CP0_IP="${CP_PRIVATE_IPS[0]}" CP1_IP="${CP_PRIVATE_IPS[1]}" CP2_IP="${CP_PRIVATE_IPS[2]}" \
        render_tpl PRIVATE_IP CP0_IP CP1_IP CP2_IP \
        <"$AWS_BENCH_ROOT/configs/nomad/server.hcl.tpl" >/tmp/nomad-server-$idx.hcl
    rcp "/tmp/nomad-server-$idx.hcl" "$ip:/tmp/server.hcl"
    rm -f "/tmp/nomad-server-$idx.hcl"
    rsh "$ip" "
        sudo mv /tmp/server.hcl /etc/nomad.d/server.hcl
        sudo tee /etc/systemd/system/nomad.service >/dev/null <<'UNIT'
[Unit]
Description=Nomad server (benchmark)
After=network-online.target

[Service]
ExecStart=/usr/local/bin/nomad agent -config /etc/nomad.d
Restart=on-failure
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT
        sudo systemctl daemon-reload && sudo systemctl enable --now nomad
    "
done

log "waiting for a nomad leader"
deadline=$(($(date +%s) + 300))
until rsh "${CP_ADDRS[0]}" "nomad operator api /v1/status/leader 2>/dev/null" | grep -q ':4647'; do
    (($(date +%s) < deadline)) || {
        echo "no nomad leader in 300s" >&2
        exit 1
    }
    sleep 5
done

log "building nomad-nodesim ($NOMAD_NODESIM_REF) on loadgen -> s3://$BUCKET/bin/"
rsh "$LOADGEN_ADDR" "
    set -euo pipefail
    command -v go >/dev/null 2>&1 || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y golang-go
    rm -rf /tmp/nomad-nodesim
    git clone --depth 1 https://github.com/hashicorp-forge/nomad-nodesim /tmp/nomad-nodesim
    cd /tmp/nomad-nodesim && git checkout '$NOMAD_NODESIM_REF' 2>/dev/null || true
    go build -o /tmp/nomad-nodesim-bin ./...  || go build -o /tmp/nomad-nodesim-bin .
    aws s3 cp --only-show-errors --region $REGION /tmp/nomad-nodesim-bin s3://$BUCKET/bin/nomad-nodesim
    git -C /tmp/nomad-nodesim rev-parse HEAD
"

log "nomad control plane up. Next: start-sim-fleet.sh nomad <nodes>"
