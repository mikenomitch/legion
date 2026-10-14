#!/usr/bin/env python3
"""Stamps, on the host's clock, when each sandbox guest is ready for exec.

Runs on a worker for the length of a measured run (run.sh starts and stops
it). The generated init starts the guest agent's exec listener (vsock port
1100) just before it runs the task's command, so the first moment a host
connection to the VM's vsock socket gets `OK` from `CONNECT 1100` is the
moment the guest is up. Unlike the guest's own clock this counts everything:
the VMM's setup, a kernel panic and reboot, and console output. Unlike the
console log file's timing it does not depend on the worker's log sink.

Each VM is probed every 5 ms until it answers, then never again.

    sudo python3 vsockwatch.py --out /tmp/vsockwatch.json &   # SIGTERM to finish

The output ({alloc_id: unix time}) is rewritten every half second, so it can
be read mid-run.
"""
import argparse
import glob
import json
import os
import signal
import socket
import time


def probe(path):
    """True once the guest agent accepts a connection on vsock port 1100."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(0.05)
    try:
        sock.connect(path)
        sock.sendall(b"CONNECT 1100\n")
        reply = sock.recv(64)
        return reply.startswith(b"OK")
    except OSError:
        return False
    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--allocs", default="/var/lib/legion/workers/*/allocs")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    stop = False

    def finish(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, finish)
    signal.signal(signal.SIGINT, finish)

    pattern = os.path.join(args.allocs, "*", ".sandbox", "vsock.sock")
    # VMs already running belong to earlier runs.
    done = set(glob.glob(pattern))
    up = {}
    last_scan = last_dump = 0.0

    def dump():
        with open(args.out + ".new", "w") as out:
            json.dump(up, out)
        os.replace(args.out + ".new", args.out)

    pending = set()
    while not stop:
        now = time.time()
        if now - last_scan > 0.02:
            pending |= set(glob.glob(pattern)) - done
            last_scan = now
        for path in list(pending):
            if probe(path):
                alloc = path.split(os.sep)[-3]
                up[alloc] = time.time()
                pending.discard(path)
                done.add(path)
        if now - last_dump > 0.5:
            dump()
            last_dump = now
        time.sleep(0.005)
    dump()


if __name__ == "__main__":
    main()
