#!/usr/bin/env python3
"""Fixture tests for analysis/sandbox_report.py: the sandbox-scaling
campaign's ceiling, per-lifecycle costs, density, and projection math.
A broken report must never turn a paid campaign into wrong numbers.

    python3 -m unittest discover benchmarks/aws/driver/tests
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "analysis"))

import sandbox_report  # noqa: E402


def summary(rate, achieved, completed, failed=0, peak=0, nodes=100):
    return {
        "mode": "instances",
        "sessions": completed + failed,
        "completed": completed,
        "failed": failed,
        "elapsed_secs": completed / achieved if achieved else 0,
        "sessions_per_sec": achieved,
        "target_rate_per_sec": rate,
        "simulated_nodes": nodes,
        "peak_running": peak,
        "stages": {
            "create_to_running": {"p50_ms": 12.0, "p99_ms": 80.0},
            "placement": {"p50_ms": 10.0, "p99_ms": 70.0},
            "dispatch": {"p50_ms": 1.0, "p99_ms": 5.0},
        },
        "errors": [],
    }


def snapshot(at, managers):
    """managers: list of (name, cpu_secs, rss_kb, raft_applied, heartbeats)."""
    return [
        {"name": n, "up": True, "cpu_secs": c, "rss_kb": r, "raft_applied": a,
         "heartbeats": h, "vcpus": 4, "at": at}
        for (n, c, r, a, h) in managers
    ]


class SandboxReportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.campaign = os.path.join(self.tmp.name, "20261012-1200-sandbox-legion-a")
        self.sandbox = os.path.join(self.campaign, "sandbox")
        os.makedirs(self.sandbox)
        self.write("campaign.json", {
            "campaign": "20261012-1200-sandbox-legion-a",
            "system": "legion-a",
            "control_plane_type": "m6id.xlarge",
            "control_plane_count": 3,
            "loadgen_type": "c7i.2xlarge",
            "region": "us-east-1",
        })
        # Three rungs: 100 and 200 hold, 400 falls short (achieved 260/s).
        for rate, achieved, completed, failed in [(100, 99.0, 2000, 0),
                                                  (200, 196.0, 4000, 0),
                                                  (400, 260.0, 7600, 400)]:
            cell = f"ladder-r{rate}"
            self.write(f"{cell}.json", summary(rate, achieved, completed, failed))
            self.write(f"{cell}.before.json", snapshot(0, [
                ("cp-0", 100.0, 200_000, 1000, 50),
                ("cp-1", 50.0, 180_000, 0, 0),
                ("cp-2", 50.0, 180_000, 0, 0)]))
            # 8 entries and 0.01 CPU-s per completed lifecycle on cp-0, plus
            # a little follower CPU.
            self.write(f"{cell}.after.json", snapshot(20, [
                ("cp-0", 100.0 + 0.01 * completed, 210_000, 1000 + 8 * completed, 50 + 1000),
                ("cp-1", 51.0, 180_000, 0, 0),
                ("cp-2", 51.0, 180_000, 0, 0)]))
        # Density: 10,000 resident, cp-0 RSS grows by 20,000 KiB.
        cell = "density-10000"
        self.write(f"{cell}.json", summary(250, 240.0, 10000, peak=10000, nodes=10))
        self.write(f"{cell}.before.json", snapshot(0, [("cp-0", 0.0, 200_000, 0, 0)]))
        self.write(f"{cell}.after.json", snapshot(100, [("cp-0", 50.0, 205_000, 80000, 500)]))
        with open(os.path.join(self.sandbox, f"{cell}.samples.jsonl"), "w") as f:
            for i, (cpu, rss, hb) in enumerate([(10.0, 210_000, 0), (14.0, 220_000, 50),
                                                (18.0, 218_000, 100)]):
                f.write(json.dumps(snapshot(10 * (i + 1), [("cp-0", cpu, rss, 0, hb)])) + "\n")

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, name, value):
        with open(os.path.join(self.sandbox, name), "w") as f:
            json.dump(value, f)

    def test_ceiling_comes_from_held_rungs_only(self):
        report = sandbox_report.build(self.campaign)
        held = {row["cell"]: row["held"] for row in report["ladder"]}
        self.assertEqual(held, {"ladder-r100": True, "ladder-r200": True, "ladder-r400": False})
        self.assertEqual(report["ceiling_per_sec"], 196.0)
        # 80% of 196 is 156.8; 196 is nearer than 99.
        self.assertEqual(report["operating_point"], "ladder-r200")

    def test_per_lifecycle_costs_sum_every_manager(self):
        report = sandbox_report.build(self.campaign)
        row = next(r for r in report["ladder"] if r["cell"] == "ladder-r100")
        self.assertAlmostEqual(row["raft_entries_per_lifecycle"], 8.0)
        # 20 CPU-s on cp-0 plus 1 + 1 on the followers, over 2,000.
        self.assertAlmostEqual(row["manager_cpu_secs_per_lifecycle"], 22.0 / 2000)
        # Busiest manager: 20 CPU-s over the run's elapsed (2000/99 s) on 4 vCPUs.
        self.assertAlmostEqual(row["busiest_manager_utilization"], 20.0 / (2000 / 99.0) / 4)

    def test_density_reads_peak_rss_growth_and_held_rates(self):
        report = sandbox_report.build(self.campaign)
        row = report["density"][0]
        self.assertEqual(row["peak_running"], 10000)
        self.assertAlmostEqual(row["rss_growth_mib"], 20_000 / 1024)
        self.assertAlmostEqual(row["manager_kib_per_resident"], 2.0)
        # 4 CPU-s per 10 s on 4 vCPUs between both sample pairs.
        self.assertAlmostEqual(row["held_busiest_manager_utilization"], 0.1)
        self.assertAlmostEqual(row["held_heartbeats_per_sec"], 5.0)

    def test_projections_are_labelled_and_use_the_operating_point(self):
        report = sandbox_report.build(self.campaign)
        by_what = {p["what"]: p for p in report["projections"]}
        self.assertAlmostEqual(by_what["lifecycles per hour at the operating point"]["value"],
                               196.0 * 3600)
        cost = by_what["control-plane cost per million lifecycles (USD)"]
        self.assertAlmostEqual(cost["value"], 0.2373 * 3 / (196.0 * 3600) * 1e6)
        self.assertIn("excludes the sandbox hosts", cost["basis"])
        resident = by_what["resident sandboxes one manager's memory holds"]
        self.assertAlmostEqual(resident["value"], 16 * 1024 * 1024 * 0.5 / 2.0)

    def test_an_unknown_instance_type_projects_no_cost_without_a_price(self):
        meta = os.path.join(self.sandbox, "campaign.json")
        with open(meta) as f:
            value = json.load(f)
        value["control_plane_type"] = "x9.mystery"
        with open(meta, "w") as f:
            json.dump(value, f)
        whats = {p["what"] for p in sandbox_report.build(self.campaign)["projections"]}
        self.assertNotIn("control-plane cost per million lifecycles (USD)", whats)
        whats = {p["what"] for p in sandbox_report.build(self.campaign, cp_hourly_usd=0.5)["projections"]}
        self.assertIn("control-plane cost per million lifecycles (USD)", whats)

    def test_no_held_rung_means_no_projection(self):
        for rate in (100, 200):
            self.write(f"ladder-r{rate}.json", summary(rate, rate * 0.5, 100, failed=5))
        report = sandbox_report.build(self.campaign)
        self.assertIsNone(report["ceiling_per_sec"])
        self.assertFalse(any("lifecycles per" in p["what"] for p in report["projections"]))

    def test_markdown_renders_every_section(self):
        report = sandbox_report.build(self.campaign)
        markdown = sandbox_report.render_markdown(report)
        for heading in ("## Ladder", "## Density", "## Projections (extrapolated, not measured)",
                        "## Caveats"):
            self.assertIn(heading, markdown)


if __name__ == "__main__":
    unittest.main()
