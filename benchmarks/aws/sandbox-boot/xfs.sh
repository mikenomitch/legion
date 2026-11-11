#!/usr/bin/env bash
# Moves each worker's state directory (allocations and the sandbox base-disk
# cache) onto a reflink-capable XFS filesystem, so a sandbox's root disk is a
# reflink of its base instead of a sparse copy. The rig's root disk is ext4,
# so the XFS lives in a loop-mounted sparse file. Existing state is copied
# across, so the worker keeps its node identity.
#
#   benchmarks/aws/sandbox-boot/xfs.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"
for name in "${SB_WORKERS[@]}"; do
    sb_ssh "$name" 'set -e
        findmnt /var/lib/legion/workers >/dev/null 2>&1 && { echo "$(hostname): already XFS"; exit 0; }
        sudo systemctl stop legion-worker
        sudo pkill -9 cloud-hyperviso || true
        sudo truncate -s 30G /var/lib/legion-xfs.img
        sudo mkfs.xfs -q -f -m reflink=1 /var/lib/legion-xfs.img
        sudo mkdir -p /mnt/legion-xfs
        sudo mount -o loop /var/lib/legion-xfs.img /mnt/legion-xfs
        sudo cp -a /var/lib/legion/workers/. /mnt/legion-xfs/
        sudo umount /mnt/legion-xfs
        sudo mount -o loop /var/lib/legion-xfs.img /var/lib/legion/workers
        sudo systemctl start legion-worker
        echo "$(hostname): $(findmnt -no FSTYPE /var/lib/legion/workers)"' &
done
wait
