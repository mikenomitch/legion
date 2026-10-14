#!/usr/bin/env python3
"""Per-session start timeline on one worker, from its journal and consoles.

Run on a worker (boot-times.sh copies it over). For every sandbox allocation
the worker started since --since, prints when the worker began preparing it,
when the root disk was ready, when the VMM was spawned, and when the guest's
command was up. "Up" is the VMM spawn plus the guest's uptime in the
`legion-bench-up <date> <uptime>` line the benchmark jobs print from their
command: the guest kernel starts within milliseconds of the spawn, and its
uptime is exact where its wall clock is not (no RTC). Times are milliseconds from the worker's "Preparing task
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default="10 min ago")
    parser.add_argument("--logs", default="/var/lib/legion/workers/*/logs")
    parser.add_argument("--json", action="store_true")
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

    rows = []
    for alloc, entry in sorted(events.items()):
        if "prepare" not in entry or "vmm" not in entry:
            continue
        logs = glob.glob(os.path.join(args.logs, str(alloc), "stdout.log"))
        if not logs:
            continue
        up = None
        with open(logs[0], errors="replace") as console:
            # A reused log dir holds earlier boots too; the last line wins.
            for line in console:
                if line.startswith("legion-bench-up "):
                    parts = line.split()
                    try:
                        up = entry["vmm"] + float(parts[2])
                    except (IndexError, ValueError):
                        pass
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
