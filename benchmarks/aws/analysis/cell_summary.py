#!/usr/bin/env python3
"""Collapse cell archives into one comparable table row each.

A cell archive answers "what happened" in a dozen files: the driver's
metrics.json, a rendered manager config, and a directory of Prometheus
range exports. Reading a five-row campaign therefore meant opening
forty-odd files and doing counter arithmetic by hand, which is how phase
2 came to quote a rejection rate reconstructed from a whole campaign's
cumulative totals rather than per cell.

    analysis/cell_summary.py results/<campaign-id>
    analysis/cell_summary.py results/*/legion-a-web_fleet-*      # any set
    analysis/cell_summary.py --json results/<campaign-id>

Every column comes from the archive itself, so a summary can be produced
long after the rig is gone.
"""

import argparse
import json
import re
import sys
from pathlib import Path

# Control-plane vCPU count, used only to turn "cores busy" into a
# percentage. The rig's control plane is m6id.xlarge; override when it
# is not.
DEFAULT_VCPUS = 4


def _load(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _series(prom, label):
    """[(labels, [(epoch, float), ...]), ...] from a Prometheus range export."""
    if not prom or prom.get("status") != "success":
        return []
    out = []
    for entry in prom.get("data", {}).get("result", []):
        values = [(int(t), float(v)) for t, v in entry.get("values", [])]
        out.append((entry.get("metric", {}).get(label), values))
    return out


def _counter_delta(prom, label):
    """{label_value: last - first} for a raw counter export.

    Counters only rise within a cell (the control plane is not restarted
    mid-cell), so last-minus-first is the cell's own total. A cell that
    began after the manager restarted would show its first sample at 0,
    which is still correct for that cell.
    """
    out = {}
    for name, values in _series(prom, label):
        if values:
            out[name] = values[-1][1] - values[0][1]
    return out


def _rate_integral(prom, label):
    """Approximate totals from a rate export, for archives predating the
    raw-counter snapshot. Sum of rate x step; marked with `~` on output
    because a 15s sample of a 1m rate is a coarse integral, not a count.
    """
    out = {}
    for name, values in _series(prom, label):
        total = 0.0
        for (t0, _), (t1, v1) in zip(values, values[1:]):
            total += v1 * (t1 - t0)
        out[name] = total
    return out


def _peak(prom, label, only=None):
    """Peak of the summed series, optionally restricted to one label value."""
    per_ts = {}
    for name, values in _series(prom, label):
        if only is not None and name != only:
            continue
        for t, v in values:
            per_ts[t] = per_ts.get(t, 0.0) + v
    return max(per_ts.values()) if per_ts else None


def _eval_threads(cell):
    """evalThreads from the cell's own rendered config — the archived
    value, never the current template, so an old cell keeps telling the
    truth about what it ran."""
    cfg = cell / "control-plane-config"
    if not cfg.is_file():
        return None
    match = re.search(r"^\s*evalThreads\s*=\s*(\d+)", cfg.read_text(), re.M)
    return int(match.group(1)) if match else None


def summarize(cell, vcpus=DEFAULT_VCPUS):
    metrics = _load(cell / "metrics.json")
    if metrics is None:
        return None
    fairness = _load(cell / "fairness-record.json") or {}
    prom = cell / "prometheus"

    applied = rejected = None
    approx = False
    counter = _counter_delta(_load(prom / "plan_apply_counter.json"), "outcome")
    if not counter:
        counter = _rate_integral(_load(prom / "plan_apply.json"), "outcome")
        approx = bool(counter)
    if counter:
        applied = counter.get("applied", 0.0)
        rejected = counter.get("rejected", 0.0)

    bind = _counter_delta(_load(prom / "plan_bind_pass.json"), "pass")
    clean = bind.get("clean")
    revalidated = bind.get("revalidated")

    cpu_cores = _peak(_load(prom / "cpu.json"), "role", only="control-plane")
    if cpu_cores is None:  # older archives group by instance only
        cpu_cores = _peak(_load(prom / "cpu.json"), "role")

    total_applies = (applied or 0) + (rejected or 0)
    return {
        "cell": cell.name,
        "nodes": fairness.get("nodes"),
        "eval_threads": _eval_threads(cell),
        "allocations": metrics.get("expected_total"),
        "placed": metrics.get("placed_total"),
        "complete": metrics.get("complete"),
        # Why the cell ended. Absent on archives predating the driver's
        # stall/cap split, where `complete: false` could mean either.
        "outcome": metrics.get("outcome"),
        "throughput_per_sec": metrics.get("throughput_per_sec"),
        "p50_ms": metrics.get("p50_ms"),
        "p99_ms": metrics.get("p99_ms"),
        "applied": applied,
        "rejected": rejected,
        "rejected_pct": (100.0 * rejected / total_applies) if total_applies else None,
        "rejected_approx": approx,
        "bind_clean": clean,
        "bind_revalidated": revalidated,
        "revalidated_pct": (
            100.0 * revalidated / (clean + revalidated)
            if clean is not None and revalidated is not None and (clean + revalidated)
            else None
        ),
        "cpu_cores_peak": cpu_cores,
        "cpu_pct_peak": (100.0 * cpu_cores / vcpus) if cpu_cores is not None else None,
        "git_rev": (fairness.get("legion_binary") or {}).get("git_rev"),
    }


def phase_breakdown(cell):
    """Mean milliseconds per eval, per scheduler plan phase.

    `legion_scheduler_plan_phase_seconds` is a histogram, so sum/count is
    the mean cost of that phase per eval. Both exports are rates over the
    same window, and dividing one rate by the other cancels the window —
    which is what makes this comparable between cells of different sizes.
    This is the view the `task_fetch grew 33x between 1k and 5k nodes`
    question needs and could not be answered from before.
    """
    sums = _series(_load(cell / "prometheus" / "plan_phase_sum.json"), "phase")
    counts = _series(_load(cell / "prometheus" / "plan_phase_rate.json"), "phase")
    totals = {}
    for name, values in sums:
        totals.setdefault(name, [0.0, 0.0])[0] = sum(v for _, v in values)
    for name, values in counts:
        totals.setdefault(name, [0.0, 0.0])[1] = sum(v for _, v in values)
    out = {}
    for name, (sum_rate, count_rate) in totals.items():
        out[name] = (1000.0 * sum_rate / count_rate) if count_rate else None
    return out


def lane_breakdown(cell):
    """Raft rounds per eval, per lane, plus what each round waited.

    This is the number the phase-3 campaign needed and did not have. Plan
    phases accounted for 41 ms/eval while each eval occupied a worker for
    ~298 ms; the difference is the quorum rounds the plan-phase histogram
    does not cover, so counting them per eval is how the eval path's raft
    cost stops being an inference.
    """
    prom = cell / "prometheus"
    ops = _counter_delta(_load(prom / "raft_ops.json"), "lane")
    evals = sum(_counter_delta(_load(prom / "evals_processed.json"), "kind").values())
    waits = {}
    for name, values in _series(_load(prom / "raft_pipeline_wait_mean.json"), "lane"):
        real = [v for _, v in values if v == v]  # drop NaN from idle windows
        if real:
            waits[name] = 1000.0 * sum(real) / len(real)
    return {
        "evals": evals or None,
        "ops_per_lane": ops,
        "ops_per_eval": {k: v / evals for k, v in ops.items()} if evals else {},
        "wait_ms_per_op": waits,
    }


def find_cells(paths):
    """A path is a cell if it holds metrics.json; otherwise its children are."""
    cells = []
    for raw in paths:
        path = Path(raw)
        if (path / "metrics.json").is_file():
            cells.append(path)
        elif path.is_dir():
            cells.extend(sorted(c for c in path.iterdir() if (c / "metrics.json").is_file()))
    return cells


def fmt(value, spec="", dash="—"):
    return dash if value is None else format(value, spec)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+", help="cell directories, or campaign directories holding them")
    ap.add_argument("--json", action="store_true", help="emit the rows as JSON instead of a table")
    ap.add_argument("--vcpus", type=int, default=DEFAULT_VCPUS, help=f"control-plane vCPUs for the CPU %% column (default {DEFAULT_VCPUS})")
    ap.add_argument("--phases", action="store_true", help="also print mean ms/eval per scheduler plan phase")
    ap.add_argument("--lanes", action="store_true", help="also print raft rounds per eval and lane wait, per lane")
    args = ap.parse_args()

    cells = find_cells(args.paths)
    if not cells:
        print("no cell archives found (a cell is a directory holding metrics.json)", file=sys.stderr)
        return 1

    rows = [r for r in (summarize(c, args.vcpus) for c in cells) if r]
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    print("| cell | nodes | threads | allocs | placed | thru/s | p50 | p99 | rejected | revalidated | CPU |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in rows:
        # A short marker rather than a column: `stalled` is the saturation
        # answer, `timeout` means the rig gave up, and both are invisible
        # in a bare placement count.
        mark = "" if r["complete"] else f" ✗{(' ' + r['outcome']) if r['outcome'] else ''}"
        placed = f"{fmt(r['placed'])}{mark}"
        rej = fmt(r["rejected_pct"], ".1f")
        if r["rejected_pct"] is not None:
            rej = ("~" if r["rejected_approx"] else "") + rej + "%"
        reval = fmt(r["revalidated_pct"], ".1f")
        if r["revalidated_pct"] is not None:
            reval += "%"
        print(
            f"| {r['cell']} | {fmt(r['nodes'])} | {fmt(r['eval_threads'])} | {fmt(r['allocations'])} "
            f"| {placed} | {fmt(r['throughput_per_sec'], '.1f')} "
            f"| {fmt(r['p50_ms'] and r['p50_ms'] / 1000, '.1f')}s | {fmt(r['p99_ms'] and r['p99_ms'] / 1000, '.1f')}s "
            f"| {rej} | {reval} | {fmt(r['cpu_pct_peak'], '.0f')}% |"
        )
    if any(r["rejected_approx"] for r in rows):
        print("\n~ = integrated from the 1m-rate export; that archive predates the raw-counter snapshot.")

    if args.phases:
        breakdowns = [(c.name, phase_breakdown(c)) for c in cells]
        phases = sorted({p for _, b in breakdowns for p in b})
        if phases:
            print("\nMean ms/eval per plan phase:\n")
            print("| cell | " + " | ".join(phases) + " |")
            print("| --- |" + " --- |" * len(phases))
            for name, breakdown in breakdowns:
                cells_out = " | ".join(fmt(breakdown.get(p), ".2f") for p in phases)
                print(f"| {name} | {cells_out} |")

    if args.lanes:
        lanes_out = [(c.name, lane_breakdown(c)) for c in cells]
        names = sorted({l for _, b in lanes_out for l in b["ops_per_eval"]})
        if names:
            print("\nRaft rounds per eval (and mean lane wait per round):\n")
            print("| cell | evals | " + " | ".join(names) + " |")
            print("| --- | --- |" + " --- |" * len(names))
            for name, b in lanes_out:
                cols = []
                for lane in names:
                    per_eval = b["ops_per_eval"].get(lane)
                    wait = b["wait_ms_per_op"].get(lane)
                    cols.append(
                        f"{fmt(per_eval, '.2f')}"
                        + (f" ({wait:.1f}ms)" if wait is not None else "")
                    )
                print(f"| {name} | {fmt(b['evals'], '.0f')} | " + " | ".join(cols) + " |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
