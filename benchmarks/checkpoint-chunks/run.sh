#!/usr/bin/env bash
# Measure how a Linux guest's memory and root disk change between
# checkpoints, at several chunk sizes (see README.md). Runs inside the image
# the Dockerfile here builds, with this directory mounted at /m. Two guests
# boot from one image; vm1 is captured five times a minute apart (GAP), vm2
# once. Captures stay inside the container; only the analysis is printed.
set -euo pipefail
KERNEL=$(ls /boot/vmlinuz-* | head -1)
INITRD=$(ls /boot/initrd.img-* | head -1)
echo "kernel=$KERNEL initrd=$INITRD"

W=/work; mkdir -p /work/out
mkdir -p $W/root
# Guest rootfs: this container's userland (debian + python3).
tar -C / --exclude=./proc --exclude=./sys --exclude=./dev --exclude=./tmp \
  --exclude=./work --exclude=./m --exclude=./boot --exclude=./var/cache/apt \
  --exclude=./var/lib/apt/lists --exclude=./usr/lib/modules --exclude=./lib/modules \
  --exclude=./usr/share/doc --exclude=./usr/share/man \
  -cf - . | tar -C $W/root -xf -
mkdir -p $W/root/proc $W/root/sys $W/root/dev $W/root/tmp $W/root/data $W/root/srv
cp /m/guest-init.sh $W/root/init.sh
cp /m/work.py $W/root/work.py
chmod +x $W/root/init.sh
echo "hello" > $W/root/srv/index.html
USED_MIB=$(du -sm $W/root | cut -f1)
SIZE_MIB=$((USED_MIB + 256))
echo "rootfs contents ${USED_MIB} MiB, image ${SIZE_MIB} MiB"
truncate -s ${SIZE_MIB}M $W/base.img
mkfs.ext4 -q -F -d $W/root $W/base.img
cp $W/base.img /work/out/base.img

boot() { # name
  local n=$1
  cp --sparse=always $W/base.img $W/$n-disk.img
  rm -f /dev/shm/$n-mem
  qemu-system-aarch64 -M virt -cpu max -accel tcg,thread=multi -smp 2 -m 512 \
    -object memory-backend-file,id=mem,size=512M,mem-path=/dev/shm/$n-mem,share=on \
    -machine memory-backend=mem \
    -kernel "$KERNEL" -initrd "$INITRD" \
    -append "root=/dev/vda rw console=ttyAMA0 init=/init.sh quiet" \
    -drive file=$W/$n-disk.img,format=raw,if=virtio,cache=writeback \
    -nic none -nographic -serial file:$W/$n-serial.log \
    -monitor unix:$W/$n-mon.sock,server,nowait -display none &
  echo $! > $W/$n.pid
}

mon() { # name cmd
  python3 - "$W/$1-mon.sock" "$2" <<'PY'
import socket, sys, time
s = socket.socket(socket.AF_UNIX); s.connect(sys.argv[1])
time.sleep(0.2); s.recv(65536)
s.sendall((sys.argv[2] + "\n").encode()); time.sleep(0.5); s.recv(65536)
PY
}

wait_ready() {
  for i in $(seq 1 600); do
    grep -q LEGION_READY $W/$1-serial.log 2>/dev/null && return 0
    sleep 1
  done
  echo "guest $1 never got ready"; tail -50 $W/$1-serial.log; exit 1
}

snap() { # name label
  mon $1 stop
  sync
  cp --sparse=always /dev/shm/$1-mem /work/out/$1-$2.mem
  cp --sparse=always $W/$1-disk.img /work/out/$1-$2.disk
  mon $1 cont
  echo "snap $1 $2 at $(date +%T)"
}

mkdir -p /work/out
boot vm1
boot vm2
wait_ready vm1
wait_ready vm2
echo "both ready at $(date +%T)"
sleep 30
snap vm1 a
snap vm2 a
sleep ${GAP:-60}
snap vm1 b
sleep ${GAP:-60}
snap vm1 c
# A "suspend after idle": the workload goes quiet, then a checkpoint.
sleep ${GAP:-60}
snap vm1 d
sleep ${GAP:-60}
snap vm1 e
kill $(cat $W/vm1.pid) $(cat $W/vm2.pid) || true
tail -5 $W/vm1-serial.log
python3 /m/analyze.py /work/out
