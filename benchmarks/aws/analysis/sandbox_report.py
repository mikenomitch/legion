#!/usr/bin/env python3
"""Report and projections for a sandbox-scaling campaign
(scripts/run-sandbox-campaign.sh).

Reads <campaign>/sandbox/: one `legion-loadgen --mode instances` summary
per cell (`ladder-r<rate>.json`, `density-<n>.json`), the before/after
snapshots of every manager (`<cell>.before.json`, `<cell>.after.json`:
CPU seconds, RSS, applied raft entries, heartbeats), the density cell's
samples (`<cell>.samples.jsonl`), and `campaign.json`. Writes
`report.json` and `report.md` next to them.

What it reports, per ladder rung: offered and achieved lifecycles/sec,
failures, create->running / placement / dispatch / suspend / resume /
destroy latency at p50 and p99, raft entries per lifecycle, and manager
CPU seconds per lifecycle. Then:

  - the CEILING: the highest achieved rate among rungs that held (every
    session completed, achieved >= 90% of offered);
  - the OPERATING POINT: the held rung nearest 80% of the ceiling, whose
    latencies are the ones to quote;
  - the DENSITY cell: peak resident sandboxes, manager memory per resident
    sandbox, and manager CPU while they were held;
  - PROJECTIONS, every one labelled as a linear extrapolation from measured
    cells: lifecycles per hour and per day at the operating point, control
    plane cost per million lifecycles, and resident sandboxes a manager's
    memory would hold.

Honesty rules, encoded:
  - Nothing is projected from a rung that did not hold.
  - Every projection names the measured number it scales and the
    assumption it makes; report.md prints them under "Projections
    (extrapolated, not measured)".
  - Sandbox boot and restore are simulated on this rig, so every latency
    is the control plane's share of a sandbox start. The report says so.

Stdlib only. Usage:
  sandbox_report.py --campaign benchmarks/aws/results/<campaign-id>
      [--cp-hourly-usd 0.2373] [--json out.json]
"""

import argparse
import glob
import json
import os
import re
import sys

# On-demand $/hour, us-east-1, for the control-plane types the rig uses.
# A projection is only as good as its price, so an unknown type is refused
# unless --cp-hourly-usd says what to use.
ON_DEMAND_USD = {
    "m6id.xlarge": 0.2373,
    "m7i.xlarge": 0.2016,
    "m6i.xlarge": 0.192,
    "c7i.xlarge": 0.1785,
    "m7i.2xlarge": 0.4032,
    "m6id.2xlarge": 0.4746,
}

# Instance memory (GiB) for the resident-capacity projection.
INSTANCE_MEMORY_GIB = {
    "m6id.xlarge": 16,
    "m7i.xlarge": 16,
    "m6i.xlarge": 16,
    "c7i.xlarge": 8,
    "m7i.2xlarge": 32,
    "m6id.2xlarge": 32,
}

# Share of a manager's memory the resident projection lets sandboxes use.
MEMORY_HEADROOM = 0.5

STAGES = [
    "create_rpc",
    "placement",
    "dispatch",
    "create_to_running",
    "suspend",
    "resume_dispatch",
    "resume",
    "destroy_rpc",
]


def load_json(path):
    with open(path) as f:
        return json.load(f)


def load_snapshot(path):
    """A snapshot is a JSON array, one object per manager."""
    if not os.path.exists(path):
        return []
    return [m for m in load_json(path) if m.get("up")]


def deltas(before, after):
    """Per-counter deltas summed over managers, plus the busiest manager's
    CPU delta. Managers are matched by name, so a manager that was down
    in one snapshot contributes nothing."""
    by_name = {m["name"]: m for m in before}
    total = {"cpu_secs": 0.0, "raft_applied": 0, "heartbeats": 0}
    busiest_cpu = 0.0
    vcpus = 0
    for m in after:
        b = by_name.get(m["name"])
        if not b:
            continue
        cpu = max(0.0, float(m["cpu_secs"]) - float(b["cpu_secs"]))
        total["cpu_secs"] += cpu
        total["raft_applied"] += max(0, int(m["raft_applied"]) - int(b["raft_applied"]))
        total["heartbeats"] += max(0, int(m["heartbeats"]) - int(b["heartbeats"]))
        if cpu > busiest_cpu:
            busiest_cpu = cpu
            vcpus = int(m.get("vcpus", 0))
    total["busiest_cpu_secs"] = busiest_cpu
    total["busiest_vcpus"] = vcpus
    return total


def rung_held(summary):
    rate = float(summary.get("target_rate_per_sec") or 0)
    return (
        int(summary.get("failed", 1)) == 0
        and rate > 0
        and float(summary.get("sessions_per_sec", 0)) >= 0.9 * rate
    )


def cell_report(sandbox_dir, cell_id):
    summary = load_json(os.path.join(sandbox_dir, f"{cell_id}.json"))
    before = load_snapshot(os.path.join(sandbox_dir, f"{cell_id}.before.json"))
    after = load_snapshot(os.path.join(sandbox_dir, f"{cell_id}.after.json"))
    d = deltas(before, after)
    completed = int(summary.get("completed", 0))
    elapsed = float(summary.get("elapsed_secs", 0)) or None
    stages = summary.get("stages", {})
    row = {
        "cell": cell_id,
        "offered_per_sec": float(summary.get("target_rate_per_sec") or 0),
        "achieved_per_sec": float(summary.get("sessions_per_sec", 0)),
        "sessions": int(summary.get("sessions", 0)),
        "completed": completed,
        "failed": int(summary.get("failed", 0)),
        "held": rung_held(summary),
        "simulated_nodes": int(summary.get("simulated_nodes", 0)),
        "peak_running": int(summary.get("peak_running", 0)),
        "latency_ms": {
            stage: {
                "p50": stages.get(stage, {}).get("p50_ms"),
                "p99": stages.get(stage, {}).get("p99_ms"),
            }
            for stage in STAGES
        },
        "raft_entries_per_lifecycle": (d["raft_applied"] / completed) if completed else None,
        "manager_cpu_secs_per_lifecycle": (d["cpu_secs"] / completed) if completed else None,
        "busiest_manager_utilization": (
            d["busiest_cpu_secs"] / elapsed / d["busiest_vcpus"]
            if elapsed and d["busiest_vcpus"]
            else None
        ),
        "heartbeats": d["heartbeats"],
        "errors": summary.get("errors", []),
    }
    return row


def density_report(sandbox_dir, cell_id):
    row = cell_report(sandbox_dir, cell_id)
    before = {m["name"]: m for m in load_snapshot(os.path.join(sandbox_dir, f"{cell_id}.before.json"))}
    samples = []
    path = os.path.join(sandbox_dir, f"{cell_id}.samples.jsonl")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    samples.append([m for m in json.loads(line) if m.get("up")])
    # Memory: the largest growth any manager showed over its own baseline.
    rss_growth_kb = 0
    for sample in samples:
        for m in sample:
            b = before.get(m["name"])
            if b:
                rss_growth_kb = max(rss_growth_kb, int(m["rss_kb"]) - int(b["rss_kb"]))
    peak = row["peak_running"]
    row["rss_growth_mib"] = rss_growth_kb / 1024.0
    row["manager_kib_per_resident"] = (rss_growth_kb / peak) if peak else None
    # CPU and heartbeats while resident: the busiest manager's rate between
    # consecutive samples, averaged over the samples taken at >= 90% of the
    # peak (the hold).
    rates = []
    hb_rates = []
    for prev, cur in zip(samples, samples[1:]):
        p = {m["name"]: m for m in prev}
        busiest = None
        hb_total = 0.0
        for m in cur:
            q = p.get(m["name"])
            if not q:
                continue
            dt = float(m["at"]) - float(q["at"])
            if dt <= 0:
                continue
            cpu_rate = (float(m["cpu_secs"]) - float(q["cpu_secs"])) / dt
            hb_total += (int(m["heartbeats"]) - int(q["heartbeats"])) / dt
            if busiest is None or cpu_rate > busiest[0]:
                busiest = (cpu_rate, int(m.get("vcpus", 0)) or 1)
        if busiest:
            rates.append(busiest[0] / busiest[1])
            hb_rates.append(hb_total)
    row["held_busiest_manager_utilization"] = (
        sorted(rates)[len(rates) // 2] if rates else None
    )
    row["held_heartbeats_per_sec"] = sorted(hb_rates)[len(hb_rates) // 2] if hb_rates else None
    return row


def build(campaign_dir, cp_hourly_usd=None):
    sandbox_dir = os.path.join(campaign_dir, "sandbox")
    meta_path = os.path.join(sandbox_dir, "campaign.json")
    meta = load_json(meta_path) if os.path.exists(meta_path) else {}
    ladder = []
    for path in glob.glob(os.path.join(sandbox_dir, "ladder-r*.json")):
        name = os.path.basename(path)
        if re.fullmatch(r"ladder-r\d+(\.\d+)?\.json", name):
            ladder.append(cell_report(sandbox_dir, name[: -len(".json")]))
    ladder.sort(key=lambda row: row["offered_per_sec"])
    density = []
    for path in glob.glob(os.path.join(sandbox_dir, "density-*.json")):
        name = os.path.basename(path)
        if re.fullmatch(r"density-\d+\.json", name):
            density.append(density_report(sandbox_dir, name[: -len(".json")]))

    held = [row for row in ladder if row["held"]]
    ceiling = max((row["achieved_per_sec"] for row in held), default=None)
    operating = None
    if ceiling:
        target = 0.8 * ceiling
        operating = min(held, key=lambda row: abs(row["achieved_per_sec"] - target))

    cp_type = meta.get("control_plane_type", "")
    managers = int(meta.get("control_plane_count") or 0)
    price = cp_hourly_usd if cp_hourly_usd is not None else ON_DEMAND_USD.get(cp_type)
    projections = []
    if operating and operating["achieved_per_sec"] > 0:
        rate = operating["achieved_per_sec"]
        projections.append({
            "what": "lifecycles per hour at the operating point",
            "value": rate * 3600,
            "basis": f"{rate:.1f} lifecycles/s measured at the {operating['cell']} rung, held for an hour",
        })
        projections.append({
            "what": "lifecycles per day at the operating point",
            "value": rate * 86400,
            "basis": f"{rate:.1f} lifecycles/s measured at the {operating['cell']} rung, held for a day",
        })
        if price and managers:
            per_million = price * managers / (rate * 3600) * 1e6
            projections.append({
                "what": "control-plane cost per million lifecycles (USD)",
                "value": per_million,
                "basis": (
                    f"{managers} x {cp_type or 'manager'} at ${price:.4f}/h on demand, "
                    f"at {rate:.1f} lifecycles/s; excludes the sandbox hosts"
                ),
            })
    for row in density:
        per = row.get("manager_kib_per_resident")
        mem = INSTANCE_MEMORY_GIB.get(cp_type)
        if per and per > 0 and mem:
            capacity = mem * 1024 * 1024 * MEMORY_HEADROOM / per
            projections.append({
                "what": "resident sandboxes one manager's memory holds",
                "value": capacity,
                "basis": (
                    f"{per:.2f} KiB of manager RSS per resident sandbox measured at "
                    f"{row['peak_running']} resident, linear, using {int(MEMORY_HEADROOM * 100)}% "
                    f"of a {mem} GiB {cp_type}"
                ),
            })

    return {
        "campaign": meta.get("campaign", os.path.basename(os.path.normpath(campaign_dir))),
        "meta": meta,
        "ladder": ladder,
        "ceiling_per_sec": ceiling,
        "operating_point": operating["cell"] if operating else None,
        "density": density,
        "projections": projections,
        "caveats": [
            "Sandbox boot and restore are simulated on this rig: every latency is the control "
            "plane's share of a sandbox start, not a booted guest.",
            "Simulated nodes run on the load box, so node count is notional capacity; the "
            "control plane under test is real.",
            "Projections are linear extrapolations from the measured cells, not measurements.",
        ],
    }


def fmt(value, digits=1):
    if value is None:
        return "–"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return f"{value:,}"


def render_markdown(report):
    meta = report["meta"]
    lines = [f"# Sandbox-scaling campaign {report['campaign']}", ""]
    if meta:
        lines.append(
            f"{meta.get('system', '?')} on {meta.get('control_plane_count', '?')} x "
            f"{meta.get('control_plane_type', '?')} ({meta.get('region', '?')}), load box "
            f"{meta.get('loadgen_type', '?')}, {meta.get('legion_version', '')}".rstrip(", ")
        )
        lines.append("")
    lines += [
        "## Ladder",
        "",
        "| Offered/s | Achieved/s | Failed | Held | create→running p50/p99 ms | placement p50/p99 | dispatch p50/p99 | suspend p50/p99 | resume p50/p99 | raft entries/lifecycle | manager CPU-s/lifecycle | busiest manager CPU |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in report["ladder"]:
        lat = row["latency_ms"]
        pair = lambda stage: f"{fmt(lat[stage]['p50'])} / {fmt(lat[stage]['p99'])}"  # noqa: E731
        util = row["busiest_manager_utilization"]
        lines.append(
            f"| {fmt(row['offered_per_sec'])} | {fmt(row['achieved_per_sec'])} | {row['failed']} | "
            f"{'yes' if row['held'] else 'no'} | {pair('create_to_running')} | {pair('placement')} | "
            f"{pair('dispatch')} | {pair('suspend')} | {pair('resume')} | "
            f"{fmt(row['raft_entries_per_lifecycle'])} | {fmt(row['manager_cpu_secs_per_lifecycle'], 4)} | "
            f"{fmt(util * 100 if util is not None else None)}% |"
        )
    lines += [
        "",
        f"**Ceiling:** {fmt(report['ceiling_per_sec'])} lifecycles/s (highest rung that held). "
        f"**Operating point** (nearest 80% of the ceiling): {report['operating_point'] or '–'}.",
        "",
    ]
    if report["density"]:
        lines += [
            "## Density",
            "",
            "| Cell | Nodes | Peak resident | Manager RSS growth MiB | KiB per resident | Busiest manager CPU while held | Heartbeats/s while held |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for row in report["density"]:
            util = row["held_busiest_manager_utilization"]
            lines.append(
                f"| {row['cell']} | {row['simulated_nodes']} | {fmt(row['peak_running'])} | "
                f"{fmt(row['rss_growth_mib'])} | {fmt(row['manager_kib_per_resident'], 2)} | "
                f"{fmt(util * 100 if util is not None else None)}% | {fmt(row['held_heartbeats_per_sec'])} |"
            )
        lines.append("")
    lines += ["## Projections (extrapolated, not measured)", ""]
    if report["projections"]:
        for p in report["projections"]:
            lines.append(f"- **{p['what']}:** {fmt(p['value'], 2 if p['value'] < 100 else 0)} — {p['basis']}")
    else:
        lines.append("- None: no rung held.")
    lines += ["", "## Caveats", ""]
    lines += [f"- {c}" for c in report["caveats"]]
    return "\n".join(lines) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--campaign", required=True, help="results/<campaign-id> directory")
    parser.add_argument("--cp-hourly-usd", type=float, default=None,
                        help="on-demand $/hour per control-plane box (default: table by type)")
    parser.add_argument("--json", default=None, help="also write the report JSON here")
    args = parser.parse_args(argv)
    report = build(args.campaign, args.cp_hourly_usd)
    sandbox_dir = os.path.join(args.campaign, "sandbox")
    with open(os.path.join(sandbox_dir, "report.json"), "w") as f:
        json.dump(report, f, indent=2)
    markdown = render_markdown(report)
    with open(os.path.join(sandbox_dir, "report.md"), "w") as f:
        f.write(markdown)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=2)
    sys.stdout.write(markdown)
    return 0


if __name__ == "__main__":
    sys.exit(main())
