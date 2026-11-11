#!/bin/sh
mount -t proc proc /proc
mount -t sysfs sys /sys
mount -t devtmpfs dev /dev 2>/dev/null
mount -t tmpfs tmp /tmp
hostname guest
mkdir -p /var/log /data
cd /srv && python3 -m http.server 8000 >/var/log/http.log 2>&1 &
python3 /work.py >/var/log/work.log 2>&1 &
sleep 5
echo LEGION_READY > /dev/console
while true; do sleep 3600; done
