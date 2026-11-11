#!/usr/bin/env bash
# Forms the sandbox-boot cluster from binaries build.sh deployed: the first
# manager bootstraps raft, the rest join it, and every worker joins with a
# worker join token. Configs are rendered here, not templated files, so one
# script shows the whole cluster's shape.
#
#   benchmarks/aws/sandbox-boot/install.sh            # form (or top up) the cluster
#   benchmarks/aws/sandbox-boot/install.sh restart    # restart everything on the deployed binary
#   benchmarks/aws/sandbox-boot/install.sh wipe       # stop everything and delete all state
#
# Worker settings come from the environment, so a run can vary them:
#   SB_LAUNCH_CONCURRENCY (0 = the default, twice the cores)
set -euo pipefail
source "$(dirname "$0")/env.sh"

mgr1="${SB_MANAGERS[0]}"
token_file="$SB_STATE/token"
on_mgr1() { sb_ssh "$mgr1" "LEGION_ADDR=http://127.0.0.1:4747 LEGION_TOKEN=$(cat "$token_file" 2>/dev/null) $*"; }

case "${1:-}" in
restart)
    for name in "${SB_MANAGERS[@]}"; do sb_ssh "$name" sudo systemctl restart legion-manager; done
    sleep 5
    for name in "${SB_WORKERS[@]}"; do sb_ssh "$name" sudo systemctl restart legion-worker; done
    exit 0
    ;;
wipe)
    for name in "${SB_WORKERS[@]}"; do
        # Sessions outlive the worker (KillMode=process), so the VMMs go too.
        sb_ssh "$name" "sudo systemctl stop legion-worker 2>/dev/null; sudo pkill -9 cloud-hyperviso; sudo pkill -9 -f 'sleep 86' ; sudo rm -rf /var/lib/legion/state* /var/lib/legion/worker* /var/lib/legion/data /var/lib/legion/content /var/lib/legion/vmdev /var/lib/legion/sandbox-* /etc/legion/join-token; true" &
    done
    for name in "${SB_MANAGERS[@]}"; do
        sb_ssh "$name" "sudo systemctl stop legion-manager 2>/dev/null; sudo rm -rf /var/lib/legion/raft /var/lib/legion/store* /var/lib/legion/bootstrap-token; true" &
    done
    wait
    rm -f "$token_file"
    sb_log "Wiped"
    exit 0
    ;;
esac

peers=""
for name in "${SB_MANAGERS[@]}"; do peers+="\"$name=$(sb_private_ip "$name"):4747\", "; done

manager_config() { # name bootstrap join_token_line
    local ip
    ip="$(sb_private_ip "$1")"
    cat <<EOF
{
  server = { port = 4747, host = "0.0.0.0", advertiseAddr = "http://$ip:4747" },
  storePath = "/var/lib/legion/store",
  workerConfigPath = "/opt/legion/configs/workers/",
  telemetry = { enabled = true, port = 4749 },
  acl = { enabled = true },
  # Session tickets are signed by the workload-identity issuer.
  workloadIdentity = { enabled = true },
  edgeBoot = { enabled = true },
  persistence = {
    type = "raft",
    raft = {
      nodeId = "$1",
      bootstrap = $2,
      advertiseAddr = "$ip:4747",
      peers = [$peers],
      expectedVoters = ${#SB_MANAGERS[@]},
      dataDir = "/var/lib/legion/raft",
      topology = "single",
      $3
    },
  },
}
EOF
}

worker_config() {
    local concurrency="${SB_LAUNCH_CONCURRENCY:-0}"
    cat <<EOF
{
  workerType = "default",
  pool = "default",
  additionalNodeChecks = [],
  attributes = {},
  edgeBoot = { acceptClaims = true, launchConcurrency = $concurrency },
}
EOF
}

install_manager() { # name bootstrap join_token_line
    manager_config "$@" | sb_ssh "$1" "sudo tee /opt/legion/configs/manager.ncl >/dev/null"
    worker_config | sb_ssh "$1" "sudo tee /opt/legion/configs/workers/default.ncl >/dev/null"
    sb_ssh "$1" "sudo tee /etc/systemd/system/legion-manager.service >/dev/null" <<'UNIT'
[Unit]
Description=Legion manager
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/var/lib/legion
ExecStart=/opt/legion/bin/legion start manager -c /opt/legion/configs/manager.ncl
Environment=RUST_LOG=info
Environment=LEGION_DATA_DIR=/var/lib/legion
Restart=on-failure
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT
    sb_ssh "$1" "sudo systemctl daemon-reload && sudo systemctl enable legion-manager && sudo systemctl restart legion-manager"
}

wait_ready() {
    for _ in $(seq 90); do
        sb_ssh "$1" "curl -fsS http://127.0.0.1:4749/readyz" >/dev/null 2>&1 && return 0
        sleep 2
    done
    echo "$1 never became ready" >&2
    sb_ssh "$1" "sudo journalctl -u legion-manager -n 40 --no-pager" >&2
    return 1
}

if ! sb_ssh "$mgr1" "test -d /var/lib/legion/raft"; then
    sb_log "Bootstrapping $mgr1"
    install_manager "$mgr1" true ""
    wait_ready "$mgr1"
fi
if [ ! -s "$token_file" ]; then
    sb_ssh "$mgr1" "sudo cat /var/lib/legion/bootstrap-token" >"$token_file"
    chmod 600 "$token_file"
fi
for name in "${SB_MANAGERS[@]:1}"; do
    sb_ssh "$name" "test -d /var/lib/legion/raft" && continue
    sb_log "Joining $name to raft"
    raft_token="$(on_mgr1 legion raft join-token | sed -n 's/^token=//p')"
    install_manager "$name" false "joinToken = \"$raft_token\","
    wait_ready "$name"
done

for name in "${SB_WORKERS[@]}"; do
    sb_ssh "$name" "systemctl is-active --quiet legion-worker" && continue
    sb_log "Joining $name"
    join="$(on_mgr1 legion join-token create --type default --ttl 24h -j | jq -r .data.token)"
    sb_ssh "$name" "sudo mkdir -p /etc/legion && sudo sh -c 'umask 077 && cat > /etc/legion/join-token'" <<<"$join"
    ip="$(sb_private_ip "$name")"
    sb_ssh "$name" "sudo tee /etc/systemd/system/legion-worker.service >/dev/null" <<UNIT
[Unit]
Description=Legion worker
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/var/lib/legion
ExecStart=/opt/legion/bin/legion start worker --token-file /etc/legion/join-token --name $name
Environment=RUST_LOG=info
Environment=LEGION_DATA_DIR=/var/lib/legion
Environment=LEGION_WORKER_HOST=$ip
Environment=LEGION_WORKER_ADVERTISE_ADDR=$ip
Environment=LEGION_GUEST_BINARY=/opt/legion/bin/legion-guest
Restart=on-failure
KillMode=process
LimitNOFILE=1048576
TasksMax=infinity

[Install]
WantedBy=multi-user.target
UNIT
    sb_ssh "$name" "sudo systemctl daemon-reload && sudo systemctl enable --now legion-worker"
done

sb_log "Waiting for ${#SB_WORKERS[@]} worker(s)"
for _ in $(seq 60); do
    ready="$(on_mgr1 legion node list -j | jq '[.data[] | select(.status == "active")] | length' 2>/dev/null || echo 0)"
    [ "$ready" -ge "${#SB_WORKERS[@]}" ] && break
    sleep 2
done
on_mgr1 legion node list
