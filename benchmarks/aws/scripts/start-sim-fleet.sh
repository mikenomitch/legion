#!/usr/bin/env bash
# Start the simulated-node fleet for one system across the current spot
# worker instances, split evenly per instance (profile mix inside each
# slice is handled by each simulator from the same profiles file).
#
# Usage:
#   start-sim-fleet.sh legion <total-nodes> [scenario.json] [manager-addr]
#   start-sim-fleet.sh nomad  <total-nodes> [scenario.json]
#   start-sim-fleet.sh k8s    <total-nodes> [scenario.json]
#
# scenario.json defaults to the web_fleet shape; only its node_profiles
# are read. Legion: one `legion start worker --simulate` systemd unit
# per instance. Nomad: one nomad-nodesim unit per instance. K8s: KWOK
# Node objects created via the loadgen kubeconfig (no worker-instance
# processes at all — KWOK fakes nodes from the controller).
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

SYSTEM="${1:?usage: start-sim-fleet.sh <legion|nomad|k8s> <total-nodes> [scenario] [manager]}"
TOTAL="${2:?total node count required}"
SCENARIO="${3:-$SCENARIOS_DIR/web_fleet.json}"
MANAGER="${4:-http://${CP_PRIVATE_IPS[0]}:4747}"

# Worker telemetry port for Legion sim fleets. node_exporter owns 9100 on
# these boxes; the sim process exports the `legion_worker_*` registry here
# (docs/metrics.md § Worker), and the legion branch below publishes the
# matching Prometheus file_sd targets. Intra-rig traffic is unrestricted
# (terraform/security.tf), so no security-group change is needed.
SIM_TELEMETRY_PORT=9101

mapfile -t WORKERS < <(worker_instances)
[ "${#WORKERS[@]}" -gt 0 ] || {
    echo "no InService worker instances — scale the fleet first (run-campaign.sh does this per step)" >&2
    exit 1
}
COUNT=${#WORKERS[@]}
BASE=$((TOTAL / COUNT))
EXTRA=$((TOTAL % COUNT))
log "starting $SYSTEM sim fleet: $TOTAL nodes over $COUNT instances (~$BASE each)"

case "$SYSTEM" in
legion)
    log "minting a sim join token via loadgen"
    TOKEN=$(rsh "$LOADGEN_ADDR" \
        "LEGION_ADDR=$MANAGER legion join-token create --type sim --ttl 24h -j" |
        jq -r '.data.token? // empty' | head -1)
    [ -n "$TOKEN" ] || TOKEN=$(rsh "$LOADGEN_ADDR" \
        "LEGION_ADDR=$MANAGER legion join-token create --type sim --ttl 24h" |
        grep -o 'lgnjoin_[A-Za-z0-9_-]*' | head -1)
    [ -n "$TOKEN" ] || {
        echo "failed to mint a join token" >&2
        exit 1
    }

    idx=0
    for line in "${WORKERS[@]}"; do
        read -r instance_id public private <<<"$line"
        addr=$(worker_addr "$public" "$private")
        slice=$BASE
        [ "$idx" -lt "$EXTRA" ] && slice=$((slice + 1))
        [ "$slice" -gt 0 ] || continue
        log "  $instance_id ($private): $slice nodes"
        wait_for_ssh "$addr" 120
        rcp "$SCENARIO" "$addr:/tmp/profiles.json"
        rsh "$addr" "
            sudo aws s3 cp --region $REGION s3://$BUCKET/bin/legion /opt/legion/bin/legion &&
            sudo chmod +x /opt/legion/bin/legion &&
            sudo mv /tmp/profiles.json /opt/legion/configs/profiles.json &&
            sudo tee /etc/systemd/system/legion-sim.service >/dev/null <<UNIT
[Unit]
Description=Legion simulated worker fleet (benchmark)
After=network-online.target

[Service]
ExecStart=/opt/legion/bin/legion start worker $TOKEN \\
    --simulate $slice \\
    --profiles /opt/legion/configs/profiles.json \\
    --name-prefix sim-$idx \\
    --advertise-host $private \\
    --register-rate 250
Restart=on-failure
LimitNOFILE=1048576
Environment=RUST_LOG=info
Environment=LEGION_WORKER_TELEMETRY_PORT=$SIM_TELEMETRY_PORT

[Install]
WantedBy=multi-user.target
UNIT
            sudo systemctl daemon-reload &&
            sudo systemctl restart legion-sim
        "
        idx=$((idx + 1))
    done

    # Publish the sim hosts as Prometheus targets. Written here rather
    # than in gen-targets.sh so the targets exist exactly while a Legion
    # sim fleet does: the Nomad and K8s campaigns reuse these same
    # instances and would otherwise leave a file of dead scrape targets
    # behind in their fairness records.
    SIM_TARGETS=$(mktemp)
    {
        echo '['
        first=true
        for line in "${WORKERS[@]}"; do
            read -r instance_id _public private <<<"$line"
            [ -n "$instance_id" ] || continue
            $first || echo ','
            first=false
            printf '  {"targets":["%s:%s"],"labels":{"role":"sim-worker","instance_name":"%s"}}' \
                "$private" "$SIM_TELEMETRY_PORT" "$instance_id"
        done
        echo
        echo ']'
    } >"$SIM_TARGETS"
    rcp "$SIM_TARGETS" "$LOADGEN_ADDR:/opt/bench/observability/targets/sim-workers.json"
    rm -f "$SIM_TARGETS"
    log "published ${#WORKERS[@]} sim-worker scrape targets on :$SIM_TELEMETRY_PORT"

    log "waiting for $TOTAL registered nodes"
    deadline=$(($(date +%s) + 600))
    while :; do
        registered=$(rsh "$LOADGEN_ADDR" \
            "LEGION_ADDR=$MANAGER legion state query 'SELECT COUNT(*) AS n FROM node' -j" |
            jq -r '.data.rows[0].n // 0' 2>/dev/null || echo 0)
        log "  registered: $registered/$TOTAL"
        [ "$registered" -ge "$TOTAL" ] && break
        (($(date +%s) < deadline)) || {
            echo "fleet did not reach $TOTAL nodes in 600s" >&2
            exit 1
        }
        sleep 10
    done
    ;;

nomad)
    idx=0
    for line in "${WORKERS[@]}"; do
        read -r instance_id public private <<<"$line"
        addr=$(worker_addr "$public" "$private")
        slice=$BASE
        [ "$idx" -lt "$EXTRA" ] && slice=$((slice + 1))
        [ "$slice" -gt 0 ] || continue
        log "  $instance_id ($private): $slice nodesim nodes"
        wait_for_ssh "$addr" 120
        rsh "$addr" "
            sudo aws s3 cp --region $REGION s3://$BUCKET/bin/nomad-nodesim /opt/legion/bin/nomad-nodesim &&
            sudo chmod +x /opt/legion/bin/nomad-nodesim &&
            sudo mkdir -p /var/lib/nodesim &&
            sudo tee /etc/systemd/system/nomad-nodesim.service >/dev/null <<UNIT
[Unit]
Description=nomad-nodesim fleet (benchmark)
After=network-online.target

[Service]
ExecStart=/opt/legion/bin/nomad-nodesim -server-addr ${CP_PRIVATE_IPS[0]}:4647 -node-num $slice -work-dir /var/lib/nodesim
Restart=on-failure
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT
            sudo systemctl daemon-reload &&
            sudo systemctl restart nomad-nodesim
        "
        idx=$((idx + 1))
    done
    log "nodesim units started; verify with: nomad node status (expect $TOTAL)"
    ;;

k8s)
    # KWOK nodes are API objects — created once, from the loadgen box,
    # shaped by the shared profile split. max pods per fake node is
    # generous: bin-packing pressure comes from cpu/memory requests.
    rcp "$SCENARIO" "$LOADGEN_ADDR:/tmp/profiles.json"
    rcp "$AWS_BENCH_ROOT/workloads/profile_split.py" "$LOADGEN_ADDR:/tmp/profile_split.py"
    rcp "$AWS_BENCH_ROOT/configs/k8s/kwok-node.yaml.tpl" "$LOADGEN_ADDR:/tmp/kwok-node.yaml.tpl"
    rsh "$LOADGEN_ADDR" "
        set -euo pipefail
        python3 /tmp/profile_split.py --profiles /tmp/profiles.json --total $TOTAL |
        while read -r name cpus memory count attrs; do
            class=\$(echo \"\$attrs\" | tr ' ' '\n' | sed -n 's/^class=//p')
            region=\$(echo \"\$attrs\" | tr ' ' '\n' | sed -n 's/^region=//p')
            for i in \$(seq 0 \$((count - 1))); do
                NODE_NAME=\"kwok-\$name-\$i\" NODE_CLASS=\"\${class:-none}\" \
                NODE_REGION=\"\${region:-none}\" CPUS=\"\$cpus\" MEMORY_GI=\"\$memory\" MAX_PODS=1000 \
                envsubst < /tmp/kwok-node.yaml.tpl
                echo '---'
            done
        done | kubectl apply -f -
    "
    log "created $TOTAL KWOK nodes; verify with: kubectl get nodes -l type=kwok"
    ;;

*)
    echo "unknown system: $SYSTEM" >&2
    exit 1
    ;;
esac
