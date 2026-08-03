#!/usr/bin/env bash
# Install Legion B on the control plane: streaming persistence with the
# S3 replica and object-storage lease (configs/legion/manager-b.jsonnet.tpl).
# All three boxes run the same config; the lease elects one primary, the
# other two stand by. Wipes previous state first (serial campaigns).
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

"$(dirname "$0")/wipe-control-plane.sh"

# A fresh Legion B campaign must not adopt a previous campaign's replica.
log "clearing s3://$BUCKET/legion-b/"
aws s3 rm --region "$REGION" "s3://$BUCKET/legion-b/" --recursive --quiet || true

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
for idx in 0 1 2; do
    ip="${CP_ADDRS[$idx]}"
    # Rendered per node, not once for all three: server.advertiseAddr is
    # the address baked into every join token this manager mints, so it
    # has to be THIS box's private ip. Sharing one rendered config would
    # hand every worker the same (wrong) manager address.
    BUCKET="$BUCKET" REGION="$REGION" PRIVATE_IP="${CP_PRIVATE_IPS[$idx]}" \
        render_tpl BUCKET REGION PRIVATE_IP \
        <"$AWS_BENCH_ROOT/configs/legion/manager-b.jsonnet.tpl" >"$STAGE/manager.jsonnet"
    log "installing legion (streaming) on cp-$idx ($ip)"
    rsh "$ip" "
        sudo aws s3 cp --region $REGION s3://$BUCKET/bin/legion /opt/legion/bin/legion &&
        sudo chmod +x /opt/legion/bin/legion &&
        sudo ln -sf /opt/legion/bin/legion /usr/local/bin/legion &&
        sudo mkdir -p /opt/legion/configs/workers /opt/legion/plugins /var/lib/legion &&
        sudo aws s3 cp --region $REGION s3://$BUCKET/bin/legion-plugins.tar.gz /tmp/plugins.tar.gz &&
        sudo tar -xzf /tmp/plugins.tar.gz --strip-components=1 -C /opt/legion/plugins
    "
    rcp "$STAGE/manager.jsonnet" "$ip:/tmp/manager.jsonnet"
    rcp "$AWS_BENCH_ROOT/configs/legion/workers/sim.jsonnet" "$ip:/tmp/sim.jsonnet"
    rsh "$ip" "
        sudo mv /tmp/manager.jsonnet /opt/legion/configs/manager.jsonnet &&
        sudo mv /tmp/sim.jsonnet /opt/legion/configs/workers/sim.jsonnet &&
        sudo tee /etc/systemd/system/legion-manager.service >/dev/null <<'UNIT'
[Unit]
Description=Legion manager (benchmark, streaming persistence)
After=network-online.target

[Service]
WorkingDirectory=/var/lib/legion
ExecStart=/opt/legion/bin/legion start manager -c /opt/legion/configs/manager.jsonnet
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
done

# One primary (the lease winner) serves; standbys are healthy processes
# that report standby via readyz. Wait until exactly one is ready.
log "waiting for the lease to elect a primary"
deadline=$(($(date +%s) + 300))
PRIMARY=""
while [ -z "$PRIMARY" ]; do
    for idx in 0 1 2; do
        # Probed the same way as install-legion-a: 4749 is intra-rig
        # only, so a direct curl from an operator box never succeeds and
        # every node would look like a standby forever. See
        # rig_http_ready in env.sh.
        if rig_probe_once "${CP_ADDRS[$idx]}" 4749/readyz; then
            PRIMARY="cp-$idx (${CP_ADDRS[$idx]})"
            break
        fi
    done
    [ -n "$PRIMARY" ] || {
        (($(date +%s) < deadline)) || {
            echo "no streaming primary within 300s" >&2
            exit 1
        }
        sleep 5
    }
done
log "Legion B is up; primary: $PRIMARY"
log "Next: start-sim-fleet.sh legion <nodes> (point it at the primary)"
