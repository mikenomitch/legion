#!/usr/bin/env python3
"""Per-session start timeline on one worker, from its journal and consoles.

Run on a worker (boot-times.sh copies it over). For every sandbox allocation
the worker started since --since, prints when the worker began preparing it,
when the root disk was ready, when the VMM was spawned, and when the guest's
command was up. "Up" is the VMM spawn plus the guest's uptime in the
`legion-bench-up <date> <uptime>` line the benchmark jobs print from their
command, plus any panic-and-reboot before it (see guest_up): the guest kernel
starts within milliseconds of the spawn, and its uptime is exact where its
wall clock is not (no RTC). Times are milliseconds from the worker's "Preparing task
allocation" line.

    sudo python3 boot-times.py --since '5 min ago' [--json]
"""
import argparse
import glob
import json
import os
import re
import statistics
import subprocess
from datetime import datetime, timezone

ANSI = re.compile(r"\x1b\[[0-9;]*m")
LINE = re.compile(r"^(\S+)Z\s+INFO\s+(\S+): (.*?), allocation_id: (-?\d+)")


def parse_ts(text):
    return datetime.strptime(text[:26], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()


KTIME = re.compile(rb"^\[\s*(\d+\.\d+)\]")
REBOOT = re.compile(rb"Rebooting in (\d+) seconds")


def guest_up(console_path, vmm_spawn):
    """When the guest's command started, on the host's clock.

    The guest's uptime at the `legion-bench-up` mark counts from its *last*
    boot. A guest that panicked and rebooted first (`panic=N`) spent that
    boot's length plus N seconds before it, so each earlier boot that ends
    in "Rebooting in N seconds" adds its last kernel timestamp and N. Reading
    the host-side log file's timing instead would measure the worker's log
    sink, which flushes late under load.
    """
    with open(console_path, "rb") as console:
        data = console.read()
    mark = data.rfind(b"legion-bench-up ")
    if mark < 0:
        return None
    line = data[mark:data.find(b"\n", mark) if b"\n" in data[mark:] else len(data)].split()
    try:
        uptime = float(line[2])
    except (IndexError, ValueError):
        return None
    # Boots of this allocation's VMM: a log dir holds only its allocation's
    # output, and every boot prints the kernel banner even under `quiet`
    # when it panics.
    before = data[:mark]
    extra = 0.0
    last_kernel_ts = 0.0
    for raw in before.splitlines():
        stamp = KTIME.match(raw)
        if stamp:
            last_kernel_ts = float(stamp.group(1))
        reboot = REBOOT.search(raw)
        if reboot:
            extra += last_kernel_ts + float(reboot.group(1))
            last_kernel_ts = 0.0
    return vmm_spawn + extra + uptime


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default="10 min ago")
    parser.add_argument("--logs", default="/var/lib/legion/workers/*/logs")
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--up",
        help="vsockwatch.py's output: host-clock stamps of each guest's agent accepting exec. "
        "Used in place of the console's timing when given; an unstamped guest is left out",
    )
    args = parser.parse_args()

    journal = subprocess.run(
        ["journalctl", "-u", "legion-worker", "--since", args.since, "-o", "cat", "--no-pager"],
        capture_output=True, text=True, check=True,
    ).stdout
    events = {}
    for raw in journal.splitlines():
        match = LINE.match(ANSI.sub("", raw).strip())
        if not match:
            continue
        ts, _module, message, alloc = match.groups()
        stamp = parse_ts(ts)
        entry = events.setdefault(int(alloc), {})
        key = {
            "Preparing task allocation": "prepare",
            "Building sandbox root disk": "build",
            "Reusing sandbox root disk from the previous start": "build",
            "Starting sandbox task": "vmm",
        }.get(message)
        if key and key not in entry:
            entry[key] = stamp

    stamped = None
    if args.up and os.path.exists(args.up):
        with open(args.up) as handle:
            stamped = {int(alloc): stamp for alloc, stamp in json.load(handle).items()}

    rows = []
    for alloc, entry in sorted(events.items()):
        if "prepare" not in entry or "vmm" not in entry:
            continue
        logs = glob.glob(os.path.join(args.logs, str(alloc), "stdout.log"))
        if not logs:
            continue
        if stamped is not None:
            up = stamped.get(alloc)
        else:
            up = guest_up(logs[0], entry["vmm"])
        if up is None:
            continue
        start = entry["prepare"]
        rows.append({
            "alloc": alloc,
            "prepare_unix": start,
            "to_build_ms": round((entry.get("build", start) - start) * 1000, 1),
            "to_vmm_ms": round((entry["vmm"] - start) * 1000, 1),
            "to_up_ms": round((up - start) * 1000, 1),
        })

    def pct(values, p):
        values = sorted(values)
        return values[min(len(values) - 1, round(p / 100 * (len(values) - 1)))] if values else None

    summary = {"sessions": len(rows)}
    for key in ("to_build_ms", "to_vmm_ms", "to_up_ms"):
        values = [row[key] for row in rows]
        summary[key] = {
            "p50": pct(values, 50), "p90": pct(values, 90), "p99": pct(values, 99),
            "max": max(values) if values else None,
            "mean": round(statistics.mean(values), 1) if values else None,
        }
    if rows:
        first = min(row["prepare_unix"] for row in rows)
        last_up = max(row["prepare_unix"] + row["to_up_ms"] / 1000 for row in rows)
        summary["window_s"] = round(last_up - first, 3)
        summary["guests_up_per_sec"] = round(len(rows) / (last_up - first), 2) if last_up > first else None
    print(json.dumps({"summary": summary, "rows": rows} if args.json else summary, indent=2))


if __name__ == "__main__":
    main()
