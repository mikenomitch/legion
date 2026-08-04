#!/usr/bin/env python3
"""Scaling-curve fits and projections for a campaign's cells.

Implements the plan's projection methodology: for each (system,
scenario, metric) series over the measured node steps, fit BOTH a
linear model (y = a + b*x) and a log-log power law (ln y = a + b*ln x),
keep whichever explains the data better (lower RMSE on the original
scale), and project to 20k/50k nodes with a 95% prediction interval.

Honesty rules, encoded:
  - A projection is reported ONLY with its interval and the model that
    made it; superlinear fits (power exponent > 1 for latency, < 1 for
    throughput) are flagged in the output rather than smoothed over.
  - If a measured validation cell exists at a projected step (the 20k
    spot burst), the report includes the delta — the projection is
    published alongside it or not at all.

Also reports the SATURATION curve when a campaign swept allocation
density at a fixed node count
(`run-campaign.sh --allocs-per-node-sweep "5 10 20 40"`): throughput and
latency per allocation count, including the points where the cluster
stopped converging. Incomplete cells are excluded from the scaling fits
but kept here on purpose — "the cell never placed its workload" is the
answer a saturation sweep is looking for, and dropping it would end the
curve exactly where it gets interesting.

Stdlib only. Usage:
  fit_curves.py --campaign benchmarks/aws/results/<campaign-id> [--json out.json]

Cell discovery expects run-step.sh's layout, in either sizing form:
  <campaign>/<system>-<scenario>-<nodes>n-a<allocations>/metrics.json
  <campaign>/<system>-<scenario>-<nodes>n-x<multiplier>/metrics.json
"""

import argparse
import glob
import json
import math
import os
import re
import sys

METRICS = ["throughput_per_sec", "p50_ms", "p95_ms", "p99_ms"]
PROJECT_TO = [20000, 50000]
# Two-sided 95% t quantiles for the tiny dfs a 4-6 step campaign has.
T_975 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306}

# Two sizing forms, because run-step.sh names a cell after the sizing it
# was actually given: `-a<allocations>` for --target-allocations (what a
# campaign uses, and the only form that distinguishes the points of an
# allocation sweep at one node count) and the older `-x<multiplier>`.
CELL_RE = re.compile(
    r"^(?P<system>.+)-(?P<scenario>[a-z0-9_]+)-(?P<nodes>\d+)n-"
    r"(?:a(?P<allocations>\d+)|x(?P<multiplier>[\d.]+))$"
)


def parse_cell_name(name):
    """`legion-a-web_fleet-5000n-a25000` -> the cell's coordinates, or None.

    `allocs_per_node` is what separates two campaigns of the same shape:
    a scaling campaign holds it constant while nodes grow, a saturation
    sweep holds nodes constant while it grows. Keying series by it keeps
    the two from being fitted as if they were one.
    """
    match = CELL_RE.match(name)
    if not match:
        return None
    nodes = int(match.group("nodes"))
    allocations = match.group("allocations")
    allocations = int(allocations) if allocations is not None else None
    return {
        "name": name,
        "system": match.group("system"),
        "scenario": match.group("scenario"),
        "nodes": nodes,
        "allocations": allocations,
        "multiplier": float(match.group("multiplier")) if match.group("multiplier") else None,
        "allocs_per_node": round(allocations / nodes, 3) if allocations and nodes else None,
    }


def ols(xs, ys):
    """Least squares y = a + b*x; returns (a, b, rmse, sxx, xbar, n)."""
    n = len(xs)
    xbar = sum(xs) / n
    ybar = sum(ys) / n
    sxx = sum((x - xbar) ** 2 for x in xs)
    if sxx == 0:
        raise ValueError("degenerate x values")
    b = sum((x - xbar) * (y - ybar) for x, y in zip(xs, ys)) / sxx
    a = ybar - b * xbar
    residuals = [y - (a + b * x) for x, y in zip(xs, ys)]
    dof = max(1, n - 2)
    s = math.sqrt(sum(r * r for r in residuals) / dof)
    return a, b, s, sxx, xbar, n


def predict_interval(a, b, s, sxx, xbar, n, x0):
    """95% prediction interval half-width at x0."""
    t = T_975.get(max(1, n - 2), 1.96)
    return t * s * math.sqrt(1 + 1 / n + (x0 - xbar) ** 2 / sxx)


def fit_series(nodes, values):
    """Fit linear and power-law models; return both plus the winner."""
    lin = ols(nodes, values)
    lin_rmse = lin[2]

    fits = {
        "linear": {
            "a": lin[0],
            "b": lin[1],
            "rmse": lin_rmse,
            "predict": lambda x, lin=lin: lin[0] + lin[1] * x,
            "interval": lambda x, lin=lin: predict_interval(*lin, x),
        }
    }

    if all(value > 0 for value in values):
        log_nodes = [math.log(x) for x in nodes]
        log_values = [math.log(y) for y in values]
        pw = ols(log_nodes, log_values)

        def pw_predict(x, pw=pw):
            return math.exp(pw[0] + pw[1] * math.log(x))

        def pw_interval(x, pw=pw):
            # Interval computed in log space, mapped back (asymmetric);
            # report the wider side as the half-width.
            half = predict_interval(*pw, math.log(x))
            mid = pw_predict(x)
            return max(mid * (math.exp(half) - 1), mid * (1 - math.exp(-half)))

        pw_rmse = math.sqrt(
            sum((pw_predict(x) - y) ** 2 for x, y in zip(nodes, values)) / max(1, len(nodes) - 2)
        )
        fits["power"] = {
            "a": math.exp(pw[0]),
            "b": pw[1],
            "rmse": pw_rmse,
            "predict": pw_predict,
            "interval": pw_interval,
        }

    winner = min(fits, key=lambda name: fits[name]["rmse"])
    return fits, winner


def load_campaign(campaign_dir):
    """-> [cell dicts], every parseable cell, complete or not."""
    cells = []
    for path in sorted(glob.glob(os.path.join(campaign_dir, "*", "metrics.json"))):
        cell = parse_cell_name(os.path.basename(os.path.dirname(path)))
        if not cell:
            continue
        with open(path) as f:
            cell["metrics"] = json.load(f)
        cell["complete"] = bool(cell["metrics"].get("complete", False))
        cells.append(cell)
    return cells


def fit_inputs(cells):
    """-> {(system, scenario, allocs_per_node): {nodes: metrics}}

    Only complete cells: a cell that never converged has no defensible
    throughput to fit. Keyed by allocation density too, so a saturation
    sweep's points cannot silently overwrite each other (they share a
    node count) or be fitted as a scaling series (they have one).
    """
    series = {}
    for cell in cells:
        if not cell["complete"]:
            print(f"note: skipping incomplete cell {cell['name']}", file=sys.stderr)
            continue
        key = (cell["system"], cell["scenario"], cell["allocs_per_node"])
        series.setdefault(key, {})[cell["nodes"]] = cell["metrics"]
    return series


def saturation(cells):
    """The allocation-density sweeps: per (system, scenario, nodes), every
    allocation count that was run, in order.

    Reported only where there is a curve to report (two or more points at
    one node count), and deliberately including the cells that did not
    converge — `complete: false` at the top of the sweep IS the saturation
    point, and its placed/expected shortfall is the measurement.
    """
    grouped = {}
    for cell in cells:
        if cell["allocations"] is None:
            continue
        grouped.setdefault((cell["system"], cell["scenario"], cell["nodes"]), []).append(cell)

    report = []
    for (system, scenario, nodes), group in sorted(grouped.items()):
        if len(group) < 2:
            continue
        points = []
        for cell in sorted(group, key=lambda c: c["allocations"]):
            metrics = cell["metrics"]
            points.append(
                {
                    "allocations": cell["allocations"],
                    "allocs_per_node": cell["allocs_per_node"],
                    "converged": cell["complete"],
                    "placed_total": metrics.get("placed_total"),
                    "expected_total": metrics.get("expected_total"),
                    "throughput_per_sec": metrics.get("throughput_per_sec"),
                    "p99_ms": metrics.get("p99_ms"),
                }
            )
        # The knee: the last point that both converged and did not lose
        # throughput against the point before it. Reported as an
        # observation, not a fit — three or four points do not support one.
        knee = None
        best = 0.0
        for point in points:
            rate = point["throughput_per_sec"] or 0.0
            if point["converged"] and rate >= best:
                best, knee = rate, point["allocations"]
        report.append(
            {
                "system": system,
                "scenario": scenario,
                "nodes": nodes,
                "points": points,
                "peak_throughput_at_allocations": knee,
            }
        )
    return report


def analyze(cells):
    report = []
    for (system, scenario, allocs_per_node), by_nodes in sorted(
        cells.items(), key=lambda item: (item[0][0], item[0][1], item[0][2] or 0)
    ):
        steps = sorted(by_nodes)
        # Validation cells (20k burst) are held out of the fit.
        fit_steps = [n for n in steps if n <= 10000]
        label = f"{system}/{scenario}"
        if allocs_per_node is not None:
            label += f" @ {allocs_per_node} allocs/node"
        if len(fit_steps) < 3:
            print(
                f"note: {label}: {len(fit_steps)} fitted steps — "
                "need >= 3 for a defensible curve; skipping",
                file=sys.stderr,
            )
            continue
        entry = {
            "system": system,
            "scenario": scenario,
            "allocs_per_node": allocs_per_node,
            "steps": fit_steps,
            "metrics": {},
        }
        for metric in METRICS:
            values = [by_nodes[n].get(metric) for n in fit_steps]
            if any(v is None for v in values):
                continue
            fits, winner = fit_series(fit_steps, values)
            chosen = fits[winner]
            projections = {}
            for target in PROJECT_TO:
                mid = chosen["predict"](target)
                half = chosen["interval"](target)
                projection = {
                    "value": round(mid, 2),
                    "interval_95": [round(mid - half, 2), round(mid + half, 2)],
                }
                if target in by_nodes and by_nodes[target].get(metric) is not None:
                    measured = by_nodes[target][metric]
                    projection["validated_against"] = measured
                    projection["validation_delta_pct"] = round(
                        100 * (measured - mid) / mid if mid else 0.0, 1
                    )
                projections[target] = projection

            flags = []
            if winner == "power":
                exponent = fits["power"]["b"]
                if metric == "throughput_per_sec" and exponent < -0.05:
                    flags.append(f"throughput decays with scale (exponent {exponent:.2f})")
                if metric.endswith("_ms") and exponent > 1.05:
                    flags.append(f"superlinear latency growth (exponent {exponent:.2f})")

            entry["metrics"][metric] = {
                "measured": dict(zip(fit_steps, values)),
                "model": winner,
                "model_params": {
                    "a": round(fits[winner]["a"], 4),
                    "b": round(fits[winner]["b"], 4),
                    "rmse": round(fits[winner]["rmse"], 4),
                },
                "projections": projections,
                "flags": flags,
            }
        report.append(entry)
    return report


def render_saturation(report):
    for entry in report:
        print(
            f"\n== saturation: {entry['system']} / {entry['scenario']} "
            f"@ {entry['nodes']} nodes"
        )
        print(f"  {'allocations':>12}  {'per node':>8}  {'throughput':>10}  {'p99 ms':>8}  placed")
        for point in entry["points"]:
            placed = f"{point['placed_total']}/{point['expected_total']}"
            if not point["converged"]:
                placed += "  DID NOT CONVERGE"
            print(
                f"  {point['allocations']:>12}  {point['allocs_per_node']:>8}  "
                f"{point['throughput_per_sec']:>10}  {point['p99_ms']:>8}  {placed}"
            )
        if entry["peak_throughput_at_allocations"] is not None:
            print(
                f"  peak sustained throughput at "
                f"{entry['peak_throughput_at_allocations']} allocations"
            )


def render(report):
    for entry in report:
        print(f"\n== {entry['system']} / {entry['scenario']} (steps: {entry['steps']})")
        for metric, data in entry["metrics"].items():
            proj = ", ".join(
                f"{target // 1000}k: {p['value']} [{p['interval_95'][0]}..{p['interval_95'][1]}]"
                + (
                    f" (measured {p['validated_against']}, Δ{p['validation_delta_pct']}%)"
                    if "validated_against" in p
                    else ""
                )
                for target, p in data["projections"].items()
            )
            line = f"  {metric:<20} {data['model']:<7} -> {proj}"
            print(line)
            for flag in data["flags"]:
                print(f"    ⚠ {flag}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True, help="results/<campaign-id> directory")
    parser.add_argument("--json", help="also write the full report as JSON here")
    args = parser.parse_args()

    cells = load_campaign(args.campaign)
    if not cells:
        print(f"no cells under {args.campaign}", file=sys.stderr)
        return 1
    scaling = analyze(fit_inputs(cells))
    saturation_report = saturation(cells)
    render(scaling)
    render_saturation(saturation_report)
    if not scaling and not saturation_report:
        print(
            f"nothing to report from {len(cells)} cell(s) under {args.campaign}: "
            "no series had 3+ complete scale steps and no node count had 2+ "
            "allocation points",
            file=sys.stderr,
        )
        return 1
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"scaling": scaling, "saturation": saturation_report}, f, indent=2)
            f.write("\n")
        print(f"\nreport written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
