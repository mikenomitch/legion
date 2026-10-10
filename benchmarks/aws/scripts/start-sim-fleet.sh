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
        # sshd answers well before cloud-init has finished: the AWS CLI
        # v2 install alone is a ~60 MB download. The very next command
        # fetches the legion binary with `aws`, so racing first boot dies
        # with a bare "sudo: aws: command not found" on a node that
        # becomes perfectly good half a minute later. This is not a
        # one-time bringup concern — the fleet is spot, so instances are
        # replaced mid-campaign and every replacement re-runs the race.
        rsh "$addr" "cloud-init status --wait >/dev/null 2>&1 || true"
        rsh "$addr" "command -v aws >/dev/null 2>&1" || {
            echo "worker $instance_id ($addr) has no aws CLI after cloud-init:" >&2
            rsh "$addr" "cloud-init status --long 2>&1 | sed 's/^/  /'" >&2 || true
            rsh "$addr" "sudo tail -20 /var/log/cloud-init-output.log 2>&1 | sed 's/^/  /'" >&2 || true
            exit 1
        }
        rcp "$SCENARIO" "$addr:/tmp/profiles.json"
        rsh "$addr" "
            sudo aws s3 cp --only-show-errors --region $REGION s3://$BUCKET/bin/legion /opt/legion/bin/legion &&
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
# Without this the fleet dials the worker's OWN 127.0.0.1:4747 (the
# default when LEGION_ADDR is unset) and every node retries a connection
# refused forever, while the campaign just sees "registered: 0/N". The
# address is the control plane's PRIVATE ip: same-VPC traffic to a public
# ip hairpins out through the internet gateway and no longer matches the
# security group's intra-rig rule.
Environment=LEGION_ADDR=$MANAGER
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

    # An EXACT census, not `registered >= TOTAL`.
    #
    # The old lower bound passed against the wrong population twice over.
    # `SELECT COUNT(*) FROM node` counts every row the control plane has
    # ever seen — nothing in Legion removes a node record — so a 4000-node
    # experiment's orphans satisfied a 1000-node fleet's wait instantly,
    # and the "fleet" the next cell measured was 1000 live nodes plus 3000
    # corpses. It also ignores health, so a fleet where every node had
    # gone `lost` passed the same gate. assert_fleet (scripts/env.sh)
    # requires the exact count, all of them active, and every name from
    # one of THIS fleet's slices, and prints the census when it does not
    # get it. $idx is the number of slices actually started.
    log "waiting for exactly $TOTAL active nodes across $idx slice(s)"
    assert_fleet "$MANAGER" "$TOTAL" "$idx" 600 || exit 1

    # Record what is now running, so the next script does not have to
    # guess: run-campaign.sh uses it to skip a redundant fleet restart
    # (every restart marks the whole fleet lost and buries the next cell
    # in reschedule evals), and run-step.sh uses the slice count to gate
    # on the same census this wait just passed.
    write_fleet_state legion "$(basename "$SCENARIO" .json)" "$TOTAL" "$idx" "$MANAGER"
    ;;

nomad)
    # One nodesim process per (instance, node profile). nodesim shapes every
    # client in a process identically — one node_class, one cpu/memory size —
    # so the shared profile split (profile_split.py, the same rule Legion's
    # --simulate and the KWOK branch use) becomes one process per profile.
    # That is what makes `class = "web"` constraints mean the same thing on
    # all three systems.
    #
    # nodesim cannot set node meta, only node_class, so scenarios that
    # constrain on anything else (mixed_heterogeneous's region and gpu) are
    # refused by translate.py for Nomad rather than silently unplaceable.
    SERVERS=$(printf '"%s:4647",' "${CP_PRIVATE_IPS[@]}")
    SERVERS="[${SERVERS%,}]"
    idx=0
    for line in "${WORKERS[@]}"; do
        read -r instance_id public private <<<"$line"
        addr=$(worker_addr "$public" "$private")
        slice=$BASE
        [ "$idx" -lt "$EXTRA" ] && slice=$((slice + 1))
        [ "$slice" -gt 0 ] || continue
        log "  $instance_id ($private): $slice nodesim nodes"
        wait_for_ssh "$addr" 120
        rsh "$addr" "cloud-init status --wait >/dev/null 2>&1 || true"

        CONFIGS=$(mktemp -d)
        units=""
        while read -r name cpus memory count attrs; do
            [ "$count" -gt 0 ] || continue
            class=$(tr ' ' '\n' <<<"$attrs" | sed -n 's/^class=//p')
            # Same units as Legion's sim fingerprint (3000 MHz per core) and
            # the translator's MHZ_PER_CORE, so a 500 MHz ask is the same
            # fraction of a node on every system.
            cat >"$CONFIGS/$name.hcl" <<HCL
node_name_prefix = "nsim-$idx-$name"
node_num         = $count
server_addr      = $SERVERS
work_dir         = "/var/lib/nodesim/$name"
log {
  level = "warn"
}
node {
  datacenter = "dc1"
  node_class = "${class}"
  resources {
    cpu_compute = $((cpus * 3000))
    memory_mb   = $((memory * 1024))
  }
}
HCL
            units="$units nomad-nodesim@$name"
        done < <(python3 "$AWS_BENCH_ROOT/workloads/profile_split.py" --profiles "$SCENARIO" --total "$slice")

        rsh "$addr" "sudo rm -rf /tmp/nodesim-configs && mkdir -p /tmp/nodesim-configs"
        for f in "$CONFIGS"/*.hcl; do rcp "$f" "$addr:/tmp/nodesim-configs/"; done
        rm -rf "$CONFIGS"
        rsh "$addr" "
            set -euo pipefail
            sudo aws s3 cp --only-show-errors --region $REGION s3://$BUCKET/bin/nomad-nodesim /opt/legion/bin/nomad-nodesim
            sudo chmod +x /opt/legion/bin/nomad-nodesim
            sudo systemctl stop 'nomad-nodesim@*' 2>/dev/null || true
            # Fresh client state every start: a stale state DB replays the
            # previous fleet's allocations into this one.
            sudo rm -rf /var/lib/nodesim /etc/nodesim
            sudo mkdir -p /var/lib/nodesim /etc/nodesim
            sudo mv /tmp/nodesim-configs/*.hcl /etc/nodesim/
            sudo tee /etc/systemd/system/nomad-nodesim@.service >/dev/null <<'UNIT'
[Unit]
Description=nomad-nodesim fleet, profile %i (benchmark)
After=network-online.target

[Service]
ExecStart=/opt/legion/bin/nomad-nodesim -config /etc/nodesim/%i.hcl
Restart=on-failure
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT
            sudo systemctl daemon-reload
            sudo systemctl restart $units
        "
        idx=$((idx + 1))
    done

    # Exact count of READY nodes. Down nodes from earlier fleets linger in
    # Nomad's node table until GC, so `ready` is the population that
    # matters, and more ready nodes than asked for is a dirty cluster.
    log "waiting for exactly $TOTAL ready nomad nodes across $idx slice(s)"
    wait_for_ready_nodes nomad "$TOTAL" 900 || exit 1
    write_fleet_state nomad "$(basename "$SCENARIO" .json)" "$TOTAL" "$idx" "http://${CP_PRIVATE_IPS[0]}:4646"
    ;;

k8s)
    # KWOK node objects, plus one KWOK controller PER WORKER INSTANCE that
    # manages only its own slice (label bench-slice=<i>). The controller is
    # what heartbeats the node leases and moves pods to Running, so a slice
    # is a process exactly as it is for Legion's --simulate and nodesim —
    # and the storm drill kills one by stopping that process, the same
    # fault on all three systems.
    #
    # The upstream in-cluster KWOK deployment cannot be used here anyway:
    # the control plane runs without a CNI (no real pods exist), so its
    # nodes never go Ready and a controller pod would never schedule.
    KUBECONFIG_LOCAL=$(mktemp)
    rsh "$LOADGEN_ADDR" "cat ~/.kube/config" >"$KUBECONFIG_LOCAL"
    rcp "$SCENARIO" "$LOADGEN_ADDR:/tmp/profiles.json"
    rcp "$AWS_BENCH_ROOT/workloads/profile_split.py" "$LOADGEN_ADDR:/tmp/profile_split.py"
    rcp "$AWS_BENCH_ROOT/configs/k8s/kwok-node.yaml.tpl" "$LOADGEN_ADDR:/tmp/kwok-node.yaml.tpl"

    idx=0
    for line in "${WORKERS[@]}"; do
        read -r instance_id public private <<<"$line"
        addr=$(worker_addr "$public" "$private")
        slice=$BASE
        [ "$idx" -lt "$EXTRA" ] && slice=$((slice + 1))
        [ "$slice" -gt 0 ] || continue
        log "  $instance_id ($private): $slice KWOK nodes"

        # Node objects first, so the controller finds them on start.
        rsh "$LOADGEN_ADDR" "
            set -euo pipefail
            python3 /tmp/profile_split.py --profiles /tmp/profiles.json --total $slice |
            while read -r name cpus memory count attrs; do
                class=\$(echo \"\$attrs\" | tr ' ' '\n' | sed -n 's/^class=//p')
                region=\$(echo \"\$attrs\" | tr ' ' '\n' | sed -n 's/^region=//p')
                for i in \$(seq 0 \$((count - 1))); do
                    NODE_NAME=\"kwok-$idx-\$name-\$i\" SLICE=$idx NODE_CLASS=\"\${class:-none}\" \
                    NODE_REGION=\"\${region:-none}\" CPUS=\"\$cpus\" MEMORY_GI=\"\$memory\" MAX_PODS=1000 \
                    envsubst < /tmp/kwok-node.yaml.tpl
                    echo '---'
                done
            done | kubectl create -f - >/dev/null
        "

        wait_for_ssh "$addr" 120
        rsh "$addr" "cloud-init status --wait >/dev/null 2>&1 || true"
        rcp "$KUBECONFIG_LOCAL" "$addr:/tmp/kubeconfig"
        # Pod IPs only have to be unique per slice; 10.64+i.0.0/16 keeps
        # them out of the VPC range.
        rsh "$addr" "
            set -euo pipefail
            if ! /opt/legion/bin/kwok --version 2>/dev/null | grep -q '$KWOK_VERSION'; then
                sudo curl -fsSL -o /opt/legion/bin/kwok \
                    https://github.com/kubernetes-sigs/kwok/releases/download/v${KWOK_VERSION}/kwok-linux-amd64
                sudo chmod +x /opt/legion/bin/kwok
            fi
            # KWOK 0.8 ships no built-in stages: started without --config it
            # exits with \"no stages found\". The release's fast stages are
            # exactly the behavior we want — node leases heartbeated, pods
            # Running immediately.
            sudo mkdir -p /etc/kwok
            sudo curl -fsSL -o /etc/kwok/stage-fast.yaml \
                https://github.com/kubernetes-sigs/kwok/releases/download/v${KWOK_VERSION}/stage-fast.yaml
            sudo mv /tmp/kubeconfig /etc/kwok/kubeconfig
            sudo tee /etc/systemd/system/kwok-slice.service >/dev/null <<UNIT
[Unit]
Description=KWOK controller for node slice $idx (benchmark)
After=network-online.target

[Service]
ExecStart=/opt/legion/bin/kwok \\\\
    --kubeconfig=/etc/kwok/kubeconfig \\\\
    --config=/etc/kwok/stage-fast.yaml \\\\
    --manage-all-nodes=false \\\\
    --manage-nodes-with-annotation-selector= \\\\
    --manage-nodes-with-label-selector=bench-slice=$idx \\\\
    --cidr=10.$((64 + idx)).0.0/16 \\\\
    --node-ip=$private \\\\
    --node-lease-duration-seconds=40
Restart=on-failure
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT
            sudo systemctl daemon-reload
            sudo systemctl restart kwok-slice
        "
        idx=$((idx + 1))
    done
    rm -f "$KUBECONFIG_LOCAL"

    log "waiting for exactly $TOTAL Ready KWOK nodes across $idx slice(s)"
    wait_for_ready_nodes k8s "$TOTAL" 900 || exit 1
    write_fleet_state k8s "$(basename "$SCENARIO" .json)" "$TOTAL" "$idx" "kubeconfig"
    ;;

*)
    echo "unknown system: $SYSTEM" >&2
    exit 1
    ;;
esac
