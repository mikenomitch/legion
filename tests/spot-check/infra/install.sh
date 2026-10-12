#!/usr/bin/env bash
# Installs Legion on the rig and forms the cluster:
#   1. ships the Linux binary to every box (S3 + presigned URL)
#   2. starts sc-mgr-1 as the raft bootstrap node and reads its first token
#   3. joins sc-mgr-2 and sc-mgr-3 with a raft join token
#   4. joins both workers with a worker join token
#
# Safe to re-run: it replaces the binary and restarts services, and it skips
# bootstrap steps that already happened. To ship a new build only:
#   SC_ONLY_BINARY=1 tests/spot-check/infra/install.sh
#
# The binary is target-linux-amd64/release/legion (see the README for the
# Docker build command), or SC_BINARY=/path/to/legion.
set -euo pipefail
source "$(dirname "$0")/env.sh"

binary="${SC_BINARY:-$SC_REPO/target-linux-amd64/release/legion}"
[ -x "$binary" ] || { echo "no Linux binary at $binary" >&2; exit 1; }
file "$binary" | grep -q "x86-64" || { echo "$binary is not an x86-64 Linux binary" >&2; exit 1; }

all=("${SC_MANAGERS[@]}" "${SC_WORKERS[@]}")
mgr1="${SC_MANAGERS[0]}"
on_mgr1() { sc_ssh "$mgr1" "LEGION_ADDR=http://127.0.0.1:4747 LEGION_TOKEN=$(cat "$SC_STATE/token" 2>/dev/null) $*"; }

# --- 1. Binary
sc_log "Uploading $(du -h "$binary" | cut -f1) binary"
aws s3 cp --quiet "$binary" "s3://$SC_BUCKET/legion"
url="$(aws s3 presign "s3://$SC_BUCKET/legion" --expires-in 3600)"
for name in "${all[@]}"; do
    sc_ssh "$name" "sudo curl -fsS -o /opt/legion/bin/legion.new '$url' && sudo install -m 0755 /opt/legion/bin/legion.new /opt/legion/bin/legion && sudo rm /opt/legion/bin/legion.new && sudo ln -sf /opt/legion/bin/legion /usr/local/bin/legion" &
done
wait
sc_log "Installed $(sc_ssh "$mgr1" legion --version)"

if [ -n "${SC_ONLY_BINARY:-}" ]; then
    # Managers first, followers before the leader, then workers
    # (docs/upgrades.md). Restarting is enough: state lives in /var/lib/legion.
    for name in "${SC_MANAGERS[@]}"; do sc_ssh "$name" sudo systemctl restart legion-manager; sleep 5; done
    for name in "${SC_WORKERS[@]}"; do sc_ssh "$name" sudo systemctl restart legion-worker; done
    sc_log "Restarted everything on the new binary"
    exit 0
fi

# --- 2. Managers
peers=""
for name in "${SC_MANAGERS[@]}"; do
    peers+="\"$name=$(sc_private_ip "$name"):4747\", "
done

render_manager() { # name bootstrap join_token_line
    sed -e "s|@@NODE_ID@@|$1|" -e "s|@@PRIVATE_IP@@|$(sc_private_ip "$1")|g" -e "s|@@BOOTSTRAP@@|$2|" \
        -e "s|@@PEERS@@|$peers|" -e "s|@@ECR@@|$SC_ECR|" -e "s|@@JOIN_TOKEN_LINE@@|$3|" \
        "$SC_DIR/infra/configs/manager.ncl.tpl"
}

install_manager() { # name bootstrap join_token_line
    render_manager "$@" | sc_ssh "$1" "sudo tee /opt/legion/configs/manager.ncl >/dev/null"
    sc_ssh "$1" "sudo tee /opt/legion/configs/workers/default.ncl >/dev/null" <"$SC_DIR/infra/configs/workers/default.ncl"
    sc_ssh "$1" "sudo tee /etc/systemd/system/legion-manager.service >/dev/null" <<'UNIT'
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
    sc_ssh "$1" "sudo systemctl daemon-reload && sudo systemctl enable --now legion-manager"
}

wait_ready() { # name
    for _ in $(seq 60); do
        sc_ssh "$1" "curl -fsS http://127.0.0.1:4749/readyz" >/dev/null 2>&1 && return 0
        sleep 2
    done
    echo "$1 never became ready; see: tests/spot-check/infra/ssh.sh $1 journalctl -u legion-manager" >&2
    return 1
}

if ! sc_ssh "$mgr1" "test -d /var/lib/legion/raft"; then
    sc_log "Bootstrapping $mgr1"
    install_manager "$mgr1" true ""
    wait_ready "$mgr1"
fi

if [ ! -s "$SC_STATE/token" ]; then
    sc_log "Fetching the first-start management token (saved to .state/token)"
    # ACLs are on by default: the manager mints the first management token
    # at first start into <dataDir>/bootstrap-token (docs/auth-and-acls.md).
    sc_ssh "$mgr1" "sudo cat /var/lib/legion/bootstrap-token" >"$SC_STATE/token"
    chmod 600 "$SC_STATE/token"
    if [ ! -s "$SC_STATE/token" ]; then
        echo "$mgr1 wrote no /var/lib/legion/bootstrap-token; see: tests/spot-check/infra/ssh.sh $mgr1 journalctl -u legion-manager" >&2
        exit 1
    fi
fi

for name in "${SC_MANAGERS[@]:1}"; do
    if sc_ssh "$name" "test -d /var/lib/legion/raft"; then continue; fi
    sc_log "Joining $name to raft"
    raft_token="$(on_mgr1 legion raft join-token | sed -n 's/^token=//p')"
    install_manager "$name" false "joinToken = \"$raft_token\","
    wait_ready "$name"
done
on_mgr1 legion raft status

# --- 3. Workers
for name in "${SC_WORKERS[@]}"; do
    if sc_ssh "$name" "systemctl is-active --quiet legion-worker"; then continue; fi
    sc_log "Joining $name"
    # Long-lived on purpose: with ACLs on, a worker re-registers with this
    # same join token every time it restarts, so an expired token strands the
    # node ("unknown or revoked join token"). See the findings doc.
    join="$(on_mgr1 legion join-token create --type default --ttl 720h -j | jq -r .data.token)"
    ip="$(sc_private_ip "$name")"
    sc_ssh "$name" "sudo tee /etc/systemd/system/legion-worker.service >/dev/null" <<UNIT
[Unit]
Description=Legion worker
After=network-online.target sc-tap0.service
Wants=network-online.target

[Service]
WorkingDirectory=/var/lib/legion
ExecStart=/opt/legion/bin/legion start worker $join --name $name
Environment=RUST_LOG=info
Environment=LEGION_DATA_DIR=/var/lib/legion
# SPOT-CHECK SHIM: bind and advertise the private IP so managers can dial
# exec and logs. See src/worker/daemon.rs and worker-plane-auth-plan.md.
Environment=LEGION_WORKER_HOST=$ip
Environment=LEGION_WORKER_ADVERTISE_ADDR=$ip
Restart=on-failure
# Tasks outlive the worker process, and the worker re-adopts them on start.
KillMode=process
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT
    sc_ssh "$name" "sudo systemctl daemon-reload && sudo systemctl enable --now legion-worker"
done

sc_log "Waiting for both workers to register"
for _ in $(seq 60); do
    ready="$(on_mgr1 legion node list -j | jq '[.data[] | select(.status == "active")] | length' 2>/dev/null || echo 0)"
    [ "$ready" -ge "${#SC_WORKERS[@]}" ] && break
    sleep 2
done
on_mgr1 legion node list

# Private images pull with a KV-held ECR credential (see manager.ncl.tpl).
"$SC_DIR/infra/refresh-ecr.sh"

sc_log "Cluster is up. Point your CLI at it with:"
echo "  eval \"\$(tests/spot-check/infra/cli-env.sh)\""
