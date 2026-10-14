#!/usr/bin/env bash
# First-boot setup for every sandbox-boot rig box (Ubuntu 24.04, x86_64).
# up.sh substitutes the @@VERSION@@ placeholders. Managers and load boxes
# carry the VMM too: it costs a few MB and lets any box serve as a worker.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -q
apt-get install -y -q curl jq e2fsprogs xfsprogs iptables sqlite3 linux-tools-common sysstat

mkdir -p /opt/legion/bin /opt/legion/configs/workers /var/lib/legion/kernels

# --- sandbox runtime: cloud-hypervisor plus an uncompressed guest kernel.
curl -fsSL -o /usr/local/bin/cloud-hypervisor \
    "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/@@CH_VERSION@@/cloud-hypervisor-static"
chmod 0755 /usr/local/bin/cloud-hypervisor
curl -fsSL -o /var/lib/legion/kernels/vmlinux \
    "https://github.com/cloud-hypervisor/linux/releases/download/@@KERNEL_RELEASE@@/vmlinux-x86_64"
modprobe kvm_intel || true

# Plenty of file descriptors and processes: a node runs hundreds of VMMs.
cat >/etc/sysctl.d/90-legion-bench.conf <<'EOF'
fs.file-max = 4194304
kernel.pid_max = 4194304
fs.inotify.max_user_instances = 8192
net.core.somaxconn = 65535
net.ipv4.ip_local_port_range = 10000 65000
EOF
sysctl --system >/dev/null

ls -l /dev/kvm
echo "sandbox-boot node bootstrap done"
