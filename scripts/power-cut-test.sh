#!/usr/bin/env bash
# Power-cut test of the raft log's purge barrier (raft-write-path plan §6.1).
#
# Runs the `power_cut_*` tests in `storage::raft::state_machine` against two
# loop-mounted ext4 images, one for the state DB and one for the raft log,
# with the kernel's background writeback turned off. A write reaches an image
# only when something fsyncs it, so copying the images is a power cut: the
# copies hold exactly what was durable. The tests call this script back with
# `cut` at that moment, then read the mounted copies.
#
# Needs root, loop devices, and ext4 (Linux). Restores the writeback sysctls
# and unmounts everything on exit.
#
#   sudo scripts/power-cut-test.sh            # build and run both tests
#   scripts/power-cut-test.sh cut             # (called by the tests)
set -euo pipefail

work="${LEGION_POWER_CUT_WORK:-${TMPDIR:-/tmp}/legion-power-cut}"

mount_image() { # image mountpoint [mount options]
    local loop
    loop="$(losetup --find --show "$1")"
    mkdir -p "$2"
    mount ${3:+-o "$3"} "$loop" "$2"
}

unmount_dir() { # mountpoint
    local loop
    if mountpoint -q "$1"; then
        loop="$(findmnt -n -o SOURCE "$1")"
        umount "$1"
        losetup -d "$loop" 2>/dev/null || true
    fi
}

if [[ "${1:-}" == "cut" ]]; then
    # The cut itself: no sync, no unmount of the live filesystems. Reading
    # the backing files sees what the loop devices were sent, which is what
    # the filesystems fsynced (plus their own journal traffic).
    for fs in state log; do
        unmount_dir "$work/cut-$fs"
        cp --sparse=always "$work/$fs.img" "$work/cut-$fs.img"
        mount_image "$work/cut-$fs.img" "$work/cut-$fs"
    done
    exit 0
fi

if [[ "$(id -u)" != 0 ]]; then
    echo "power-cut-test: needs root (loop devices, mount, vm sysctls)" >&2
    exit 1
fi

writeback="$(cat /proc/sys/vm/dirty_writeback_centisecs)"
expire="$(cat /proc/sys/vm/dirty_expire_centisecs)"
cleanup() {
    echo "$writeback" > /proc/sys/vm/dirty_writeback_centisecs
    echo "$expire" > /proc/sys/vm/dirty_expire_centisecs
    for dir in cut-state cut-log state log; do
        unmount_dir "$work/$dir"
    done
    rm -f "$work"/*.img
}
trap cleanup EXIT

mkdir -p "$work"
for fs in state log; do
    unmount_dir "$work/$fs"
    rm -f "$work/$fs.img"
    truncate -s 256M "$work/$fs.img"
    mkfs.ext4 -q -F "$work/$fs.img"
    # commit=3600: no periodic journal commit pushes data out behind the
    # test's back; only an fsync (or a full journal) does.
    mount_image "$work/$fs.img" "$work/$fs" commit=3600
done
# Build before writeback is switched off, so the build's own dirty pages
# are not what fills memory during the test.
cargo test -p legion-manager --lib --no-run --locked
sync
echo 0 > /proc/sys/vm/dirty_writeback_centisecs
echo 360000 > /proc/sys/vm/dirty_expire_centisecs

LEGION_POWER_CUT_STATE="$work/state" \
LEGION_POWER_CUT_LOG="$work/log" \
LEGION_POWER_CUT_CUT_STATE="$work/cut-state" \
LEGION_POWER_CUT_CUT_LOG="$work/cut-log" \
LEGION_POWER_CUT_HOOK="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")" \
LEGION_POWER_CUT_WORK="$work" \
    cargo test -p legion-manager --lib --locked -- --ignored --test-threads=1 --nocapture power_cut_
