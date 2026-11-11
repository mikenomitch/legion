#!/usr/bin/env bash
# First-boot setup for a spot-check worker (Ubuntu 24.04, x86_64).
# up.sh substitutes the @@VERSION@@ placeholders before launch.
#
# Installs what each runtime needs on the host. Legion itself is installed
# later by install.sh, so this file never needs the binary.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -q
apt-get install -y -q curl jq python3 e2fsprogs iptables

mkdir -p /opt/legion/bin /opt/legion/configs /var/lib/legion/kernels

# --- container runtime: runc, Legion's default OCI runtime on Linux. The
# static release binary, since the default user namespace needs runc 1.2+.
curl -fsSL -o /usr/local/bin/runc \
    "https://github.com/opencontainers/runc/releases/download/@@RUNC_VERSION@@/runc.amd64"
chmod 0755 /usr/local/bin/runc

# --- sandbox runtime: cloud-hypervisor plus an uncompressed guest kernel.
curl -fsSL -o /usr/local/bin/cloud-hypervisor \
    "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/@@CH_VERSION@@/cloud-hypervisor-static"
chmod 0755 /usr/local/bin/cloud-hypervisor
curl -fsSL -o /var/lib/legion/kernels/vmlinux \
    "https://github.com/cloud-hypervisor/linux/releases/download/@@KERNEL_RELEASE@@/vmlinux-x86_64"
modprobe kvm_intel || true

# --- the EBS data disk (the redis test's "AWS storage"), mounted at /data.
root_disk="$(lsblk -no PKNAME "$(findmnt -no SOURCE /)")"
data_disk="$(lsblk -dpno NAME,TYPE | awk -v root="/dev/$root_disk" '$2 == "disk" && $1 != root { print $1; exit }')"
if [ -n "$data_disk" ]; then
    blkid "$data_disk" || mkfs.ext4 -q -L sc-data "$data_disk"
    mkdir -p /data
    echo "LABEL=sc-data /data ext4 defaults,nofail 0 2" >>/etc/fstab
    mount /data
fi
mkdir -p /data/redis /data/pipeline

# --- a bridge for the HTTP-reachable microVM test.
#
# Legion creates a TAP per sandbox on br0 and leases the guest an address from
# the worker config's sandbox.bridges.br0 pool (docs/task-runtime-sandbox.md,
# Addressing). The pool there is the single address 172.30.0.2, so this is the
# operator-side half: the bridge with its gateway address, NAT out, and host
# port 8090 forwarded to the guest's port 8080. It runs as a unit so it
# survives reboots.
cat >/etc/systemd/system/sc-br0.service <<'UNIT'
[Unit]
Description=Spot-check bridge for microVM sandboxes
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/sh -c 'ip link show br0 >/dev/null 2>&1 || ip link add br0 type bridge'
ExecStart=/bin/sh -c 'ip addr replace 172.30.0.1/24 dev br0 && ip link set br0 up'
ExecStart=/sbin/sysctl -w net.ipv4.ip_forward=1
ExecStart=/bin/sh -c 'iptables -t nat -C PREROUTING -p tcp --dport 8090 -j DNAT --to-destination 172.30.0.2:8080 2>/dev/null || iptables -t nat -A PREROUTING -p tcp --dport 8090 -j DNAT --to-destination 172.30.0.2:8080'
ExecStart=/bin/sh -c 'iptables -t nat -C POSTROUTING -s 172.30.0.0/24 ! -o br0 -j MASQUERADE 2>/dev/null || iptables -t nat -A POSTROUTING -s 172.30.0.0/24 ! -o br0 -j MASQUERADE'
ExecStart=/bin/sh -c 'iptables -C FORWARD -i br0 -j ACCEPT 2>/dev/null || iptables -A FORWARD -i br0 -j ACCEPT'
ExecStart=/bin/sh -c 'iptables -C FORWARD -o br0 -j ACCEPT 2>/dev/null || iptables -A FORWARD -o br0 -j ACCEPT'

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now sc-br0.service

ls -l /dev/kvm
echo "spot-check worker bootstrap done"
