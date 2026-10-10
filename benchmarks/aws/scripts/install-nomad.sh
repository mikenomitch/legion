#!/usr/bin/env bash
# Install Nomad (3 raft servers) on the control plane for the phase-3
# campaign, and build nomad-nodesim into S3 for the fake fleet.
# Kept in lockstep with configs/nomad/* and the pins in versions.env.
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
# Plain HTTP rather than `nomad operator api`, which Nomad 2.0 answers
# with "Invalid method" — the wait then times out on a healthy cluster.
until rsh "${CP_ADDRS[0]}" "curl -fsS -m 5 http://127.0.0.1:4646/v1/status/leader 2>/dev/null" | grep -q ':4647'; do
    (($(date +%s) < deadline)) || {
        echo "no nomad leader in 300s" >&2
        exit 1
    }
    sleep 5
done

# The driver submits and watches from the loadgen box, and the CLI path
# (the cross-system fairness baseline) shells out to `nomad job run`, so
# the loadgen box needs the same Nomad release as the servers.
log "installing the nomad $NOMAD_VERSION CLI on loadgen"
rsh "$LOADGEN_ADDR" "
    set -euo pipefail
    command -v unzip >/dev/null 2>&1 || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq unzip
    curl -fsSL -o /tmp/nomad.zip \
        https://releases.hashicorp.com/nomad/${NOMAD_VERSION}/nomad_${NOMAD_VERSION}_linux_amd64.zip
    sudo unzip -o -q /tmp/nomad.zip -d /usr/local/bin && rm /tmp/nomad.zip
    nomad version
"

# Built once per pinned ref and cached in S3 by that ref, the same way
# build-and-upload.sh caches the plugin bundle: a Go toolchain download
# plus a full Nomad-client build is minutes of rig time that cannot change
# between campaigns on the same ref.
#
# Go comes from go.dev rather than apt. nodesim's go.mod asks for Go
# 1.26.5 and Ubuntu's golang-go is years behind it, so the apt route
# fails at `go build` with a toolchain error that names neither cause.
CACHED_REF=$(aws s3 cp --only-show-errors --region "$REGION" \
    "s3://$BUCKET/bin/nomad-nodesim.ref" - 2>/dev/null || true)
if [ "$CACHED_REF" = "$NOMAD_NODESIM_REF" ]; then
    log "nomad-nodesim $NOMAD_NODESIM_REF already staged in s3://$BUCKET/bin/ — not rebuilding"
else
    log "building nomad-nodesim ($NOMAD_NODESIM_REF) on loadgen with Go $GO_VERSION -> s3://$BUCKET/bin/"
    rsh "$LOADGEN_ADDR" "
        set -euo pipefail
        if ! /usr/local/go/bin/go version 2>/dev/null | grep -q 'go$GO_VERSION '; then
            curl -fsSL -o /tmp/go.tgz https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz
            sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tgz && rm /tmp/go.tgz
        fi
        export PATH=/usr/local/go/bin:\$PATH GOTOOLCHAIN=local
        rm -rf /tmp/nomad-nodesim
        git clone -q https://github.com/hashicorp-forge/nomad-nodesim /tmp/nomad-nodesim
        cd /tmp/nomad-nodesim
        # A failed checkout must fail the install. It used to be
        # '|| true', which silently built main and recorded the pin.
        git checkout -q '$NOMAD_NODESIM_REF'
        # hashicorpmetrics is the tag nodesim's README builds with.
        go build -tags hashicorpmetrics -o /tmp/nomad-nodesim-bin .
        aws s3 cp --only-show-errors --region $REGION /tmp/nomad-nodesim-bin s3://$BUCKET/bin/nomad-nodesim
        echo -n '$NOMAD_NODESIM_REF' | aws s3 cp --only-show-errors --region $REGION - s3://$BUCKET/bin/nomad-nodesim.ref
    "
fi

log "nomad control plane up. Next: start-sim-fleet.sh nomad <nodes>"
