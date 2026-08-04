#!/usr/bin/env bash
# Install Legion A on the control plane: 3-voter raft, lanes topology
# (configs/legion/manager-a.ncl.tpl). Wipes any previous system's
# state on the boxes first — campaigns run serially on the same
# hardware, that's the methodology.
#
# Order: cp-0 bootstraps, then a raft join token minted on cp-0 enrolls
# cp-1 and cp-2. Exit: all three /readyz and 3 voters on every lane.
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

"$(dirname "$0")/wipe-control-plane.sh"

render_config() { # render_config <node-idx> <bootstrap> <join-token-line>
    NODE_ID="cp-$1" \
        BOOTSTRAP="$2" \
        JOIN_TOKEN_LINE="$3" \
        PRIVATE_IP="${CP_PRIVATE_IPS[$1]}" \
        CP0_IP="${CP_PRIVATE_IPS[0]}" \
        CP1_IP="${CP_PRIVATE_IPS[1]}" \
        CP2_IP="${CP_PRIVATE_IPS[2]}" \
        render_tpl NODE_ID BOOTSTRAP JOIN_TOKEN_LINE PRIVATE_IP CP0_IP CP1_IP CP2_IP \
        <"$AWS_BENCH_ROOT/configs/legion/manager-a.ncl.tpl"
}

install_manager() { # install_manager <node-idx> <config-file>
    local idx="$1" config="$2" ip="${CP_ADDRS[$1]}"
    log "installing legion manager on cp-$idx ($ip)"
    rsh "$ip" "
        sudo aws s3 cp --only-show-errors --region $REGION s3://$BUCKET/bin/legion /opt/legion/bin/legion &&
        sudo chmod +x /opt/legion/bin/legion &&
        sudo ln -sf /opt/legion/bin/legion /usr/local/bin/legion &&
        sudo mkdir -p /opt/legion/configs/workers /opt/legion/plugins /var/lib/legion &&
        sudo aws s3 cp --only-show-errors --region $REGION s3://$BUCKET/bin/legion-plugins.tar.gz /tmp/plugins.tar.gz &&
        sudo tar -xzf /tmp/plugins.tar.gz --strip-components=1 -C /opt/legion/plugins
    "
    rcp "$config" "$ip:/tmp/manager.ncl"
    rcp "$AWS_BENCH_ROOT/configs/legion/workers/sim.ncl" "$ip:/tmp/sim.ncl"
    rsh "$ip" "
        sudo mv /tmp/manager.ncl /opt/legion/configs/manager.ncl &&
        sudo mv /tmp/sim.ncl /opt/legion/configs/workers/sim.ncl &&
        sudo tee /etc/systemd/system/legion-manager.service >/dev/null <<'UNIT'
[Unit]
Description=Legion manager (benchmark)
After=network-online.target

[Service]
WorkingDirectory=/var/lib/legion
ExecStart=/opt/legion/bin/legion start manager -c /opt/legion/configs/manager.ncl
Restart=on-failure
LimitNOFILE=1048576
Environment=RUST_LOG=info
Environment=LEGION_PLUGIN_DIR=/opt/legion/plugins

[Install]
WantedBy=multi-user.target
UNIT
        sudo systemctl daemon-reload &&
        sudo systemctl enable --now legion-manager
    "
}

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

# cp-0: bootstrap.
render_config 0 true "" >"$STAGE/cp-0.ncl"
install_manager 0 "$STAGE/cp-0.ncl"
log "waiting for cp-0 readyz"
rig_http_ready "${CP_ADDRS[0]}" 4749/readyz 180

# Join token minted on the bootstrap node enrolls the other two.
log "minting raft join token on cp-0"
RAFT_TOKEN=$(rsh "${CP_ADDRS[0]}" "legion raft join-token" | grep -o 'lgnraft_[A-Za-z0-9_-]*' | head -1)
[ -n "$RAFT_TOKEN" ] || {
    echo "failed to mint a raft join token" >&2
    exit 1
}

for idx in 1 2; do
    render_config "$idx" false "joinToken = \"$RAFT_TOKEN\"," >"$STAGE/cp-$idx.ncl"
    install_manager "$idx" "$STAGE/cp-$idx.ncl"
done

log "waiting for all managers ready (voters settled on every lane)"
for idx in 0 1 2; do
    rig_http_ready "${CP_ADDRS[$idx]}" 4749/readyz 300
done
rsh "${CP_ADDRS[0]}" "legion raft status" || true

log "Legion A is up. Next: start-sim-fleet.sh legion <nodes>, then run-step.sh"
