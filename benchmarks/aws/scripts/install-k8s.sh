#!/usr/bin/env bash
# Install Kubernetes (kubeadm, 3 stacked-etcd control planes) + KWOK for
# the phase-4 campaign. Kept in lockstep with configs/k8s/* and the pins
# in versions.env. See configs/k8s/README.md for the
# design notes (no CNI on purpose; KWOK fakes the data plane).
set -euo pipefail
source "$(dirname "$0")/env.sh"
load_rig

"$(dirname "$0")/wipe-control-plane.sh"

K8S_MINOR="${K8S_VERSION%.*}"

install_kube_packages() { # install_kube_packages <public-ip>
    rsh "$1" "
        set -euo pipefail
        # containerd + kubeadm/kubelet/kubectl from the pkgs.k8s.io repo.
        sudo DEBIAN_FRONTEND=noninteractive apt-get install -y containerd apt-transport-https ca-certificates curl gpg
        sudo mkdir -p /etc/apt/keyrings
        curl -fsSL https://pkgs.k8s.io/core:/stable:/v${K8S_MINOR}/deb/Release.key |
            sudo gpg --yes --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
        echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v${K8S_MINOR}/deb/ /' |
            sudo tee /etc/apt/sources.list.d/kubernetes.list
        sudo apt-get update
        sudo DEBIAN_FRONTEND=noninteractive apt-get install -y kubelet=${K8S_VERSION}-\* kubeadm=${K8S_VERSION}-\* kubectl=${K8S_VERSION}-\*
        sudo apt-mark hold kubelet kubeadm kubectl
        # kubeadm preflight: swap off, br_netfilter, ip_forward.
        sudo swapoff -a
        sudo modprobe br_netfilter || true
        echo 1 | sudo tee /proc/sys/net/ipv4/ip_forward >/dev/null
        sudo mkdir -p /etc/containerd
        containerd config default | sudo tee /etc/containerd/config.toml >/dev/null
        sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
        sudo systemctl restart containerd
    "
}

log "installing kube packages on all control-plane boxes"
for ip in "${CP_ADDRS[@]}"; do
    install_kube_packages "$ip"
done

log "kubeadm init on cp-0"
CP0_IP="${CP_PRIVATE_IPS[0]}" PRIVATE_IP="${CP_PRIVATE_IPS[0]}" K8S_VERSION="$K8S_VERSION" \
    render_tpl CP0_IP PRIVATE_IP K8S_VERSION \
    <"$AWS_BENCH_ROOT/configs/k8s/kubeadm-config.yaml.tpl" >/tmp/kubeadm-config.yaml
rcp /tmp/kubeadm-config.yaml "${CP_ADDRS[0]}:/tmp/kubeadm-config.yaml"
rm -f /tmp/kubeadm-config.yaml
rsh "${CP_ADDRS[0]}" "
    sudo kubeadm init --config /tmp/kubeadm-config.yaml --upload-certs | tee /tmp/kubeadm-init.log
    # Copied after init, which is what creates /etc/kubernetes; collect.sh
    # archives this file as the cell's rendered control-plane config.
    sudo cp /tmp/kubeadm-config.yaml /etc/kubernetes/kubeadm-config.yaml
"

JOIN_CMD=$(rsh "${CP_ADDRS[0]}" "sudo kubeadm token create --print-join-command")
CERT_KEY=$(rsh "${CP_ADDRS[0]}" "sudo kubeadm init phase upload-certs --upload-certs 2>/dev/null | tail -1")

for idx in 1 2; do
    log "joining cp-$idx as a control plane"
    rsh "${CP_ADDRS[$idx]}" "sudo $JOIN_CMD --control-plane --certificate-key $CERT_KEY"
done

log "installing the kubeconfig + kubectl $K8S_VERSION on loadgen"
rsh "${CP_ADDRS[0]}" "sudo cat /etc/kubernetes/admin.conf" >/tmp/bench-kubeconfig
rcp /tmp/bench-kubeconfig "$LOADGEN_ADDR:/tmp/kubeconfig"
rm -f /tmp/bench-kubeconfig
rsh "$LOADGEN_ADDR" "
    set -euo pipefail
    mkdir -p ~/.kube && mv /tmp/kubeconfig ~/.kube/config && chmod 600 ~/.kube/config
    # Re-installed when the version differs: a kubectl left over from an
    # older campaign is outside the supported skew for a 1.36 apiserver.
    if ! kubectl version --client 2>/dev/null | grep -q 'v${K8S_VERSION}'; then
        curl -fsSL -o /tmp/kubectl https://dl.k8s.io/release/v${K8S_VERSION}/bin/linux/amd64/kubectl
        sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl
    fi
    kubectl get --raw /readyz >/dev/null
"
# KWOK is NOT deployed in-cluster: start-sim-fleet.sh runs one KWOK
# controller per worker instance, each managing its own node slice, so the
# storm drill can kill a slice the way it does for the other systems.
# Each controller runs the release's stage-fast.yaml (pods Running
# immediately, node leases heartbeated).

log "wiring scheduler metrics scrape into Prometheus"
TOKEN=$(rsh "$LOADGEN_ADDR" "
    kubectl create serviceaccount bench-metrics -n kube-system --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    kubectl create clusterrolebinding bench-metrics --clusterrole=system:monitoring --serviceaccount=kube-system:bench-metrics --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    kubectl create token bench-metrics -n kube-system --duration 48h
")
# Replace, never stack: the block is always last in the file, so drop any
# earlier copy before appending. A rerun used to append a second copy, and
# Prometheus refuses a config with duplicate job names — which, with the
# reload's curl under `set -e`, killed the whole campaign over metrics.
rsh "$LOADGEN_ADDR" "
    sed -i '/^  - job_name: kube-scheduler/,\$d' /opt/bench/observability/prometheus.yml
    cat >>/opt/bench/observability/prometheus.yml <<SCRAPE

  - job_name: kube-scheduler
    scheme: https
    tls_config: { insecure_skip_verify: true }
    authorization: { credentials: $TOKEN }
    static_configs:
      - targets: ['${CP_PRIVATE_IPS[0]}:10259', '${CP_PRIVATE_IPS[1]}:10259', '${CP_PRIVATE_IPS[2]}:10259']
  - job_name: kube-apiserver
    scheme: https
    tls_config: { insecure_skip_verify: true }
    authorization: { credentials: $TOKEN }
    static_configs:
      - targets: ['${CP_PRIVATE_IPS[0]}:6443']
SCRAPE
    curl -fsS -X POST http://localhost:9090/-/reload ||
        echo 'WARNING: Prometheus rejected the K8s scrape config; this campaign will lack scheduler phase metrics' >&2
"

log "K8s control plane up. Next: start-sim-fleet.sh k8s <nodes>"
