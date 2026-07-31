#!/usr/bin/env python3
"""Fixture tests for the driver's counting and latency math — the
"broken driver never burns cluster-hours" gate from the AWS benchmark
plan's testing section. Stdlib unittest only; run with:

    python3 -m unittest discover benchmarks/aws/driver/tests

(scripts/local-smoke.sh runs this before anything else, and CI runs it
on benchmark-file changes.)
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "workloads"))

from driver import compute_metrics, parse_legion_event, percentile  # noqa: E402
import translate  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
SCENARIOS = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "scheduler", "scenarios"
)


def load_fixture_run(name):
    with open(os.path.join(FIXTURES, name, "submits.json")) as f:
        submits = {row["job"]: row["t_ms"] for row in json.load(f)}
    events = []
    with open(os.path.join(FIXTURES, name, "driver-events.ndjson")) as f:
        for line in f:
            if line.strip():
                events.append(json.loads(line))
    return submits, events


class PercentileMath(unittest.TestCase):
    def test_nearest_rank_matches_rust_pctl(self):
        # Same convention as tests/benchmark_support/mod.rs::pctl —
        # round((len-1) * fraction) on the sorted list.
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        self.assertEqual(percentile(values, 0.50), 30.0)
        self.assertEqual(percentile(values, 0.95), 50.0)
        self.assertEqual(percentile(values, 0.0), 10.0)
        self.assertEqual(percentile([], 0.5), 0.0)

    def test_single_sample(self):
        self.assertEqual(percentile([42.0], 0.99), 42.0)


class SteadyStateFixture(unittest.TestCase):
    """steady-run: 3 jobs, 10 allocations, known-good hand-computed
    numbers. Any drift in the math fails here, not on AWS."""

    def setUp(self):
        self.submits, self.events = load_fixture_run("steady-run")

    def test_counts_and_completeness(self):
        metrics = compute_metrics(self.submits, self.events, expected_total=10)
        self.assertEqual(metrics["placed_total"], 10)
        self.assertTrue(metrics["complete"])
        self.assertEqual(metrics["latency_samples"], 10)

    def test_latency_percentiles(self):
        # Latencies by construction (weighted by event count):
        # api-0: placed at 1200 (4 allocs, submit 1000)  -> 200 x4
        # api-1: placed at 1900 (4 allocs, submit 1100)  -> 800 x4
        # cron-0: placed at 1450+1650 (1 alloc each, submit 1250)
        #         -> 200, 400
        metrics = compute_metrics(self.submits, self.events, expected_total=10)
        self.assertEqual(metrics["p50_ms"], 200.0)
        self.assertEqual(metrics["p95_ms"], 800.0)
        self.assertEqual(metrics["p99_ms"], 800.0)

    def test_throughput_window(self):
        # No warmup: window = first placement 1200 -> last 1900 = 0.7s,
        # 10 allocations in window.
        metrics = compute_metrics(self.submits, self.events, expected_total=10)
        self.assertEqual(metrics["steady_window_placed"], 10)
        self.assertAlmostEqual(metrics["steady_window_secs"], 0.7)
        self.assertAlmostEqual(metrics["throughput_per_sec"], 14.29, places=2)

    def test_warmup_discard(self):
        # 0.3s warm-up from first submit (1000) discards everything
        # before 1300: api-0's event (1200) drops, 6 allocations remain,
        # window 1450 -> 1900.
        metrics = compute_metrics(
            self.submits, self.events, expected_total=10, warmup_secs=0.3
        )
        self.assertEqual(metrics["steady_window_placed"], 6)
        self.assertEqual(metrics["latency_samples"], 6)
        self.assertAlmostEqual(metrics["steady_window_secs"], 0.45)
        self.assertFalse(metrics["complete"] and metrics["placed_total"] != 10)

    def test_incomplete_run_is_reported(self):
        metrics = compute_metrics(self.submits, self.events, expected_total=12)
        self.assertFalse(metrics["complete"])
        self.assertEqual(metrics["placed_total"], 10)


class StormFixture(unittest.TestCase):
    """storm-run: 6 initial placements, kill at t=5000, 6 replacements
    finishing at t=9000 -> recovery 4.0s."""

    def setUp(self):
        self.submits, self.events = load_fixture_run("storm-run")

    def test_recovery_time(self):
        metrics = compute_metrics(
            self.submits, self.events, expected_total=6, kill_time_ms=5000
        )
        self.assertEqual(metrics["storm"]["replaced_total"], 6)
        self.assertAlmostEqual(metrics["storm"]["recovery_secs"], 4.0)

    def test_placement_gap(self):
        # Failover-drill reading: first post-fault placement at 6200.
        metrics = compute_metrics(
            self.submits, self.events, expected_total=6, kill_time_ms=5000
        )
        self.assertAlmostEqual(metrics["storm"]["placement_gap_secs"], 1.2)

    def test_no_replacements_reports_none(self):
        pre_kill = [event for event in self.events if event["t_ms"] < 5000]
        metrics = compute_metrics(
            self.submits, pre_kill, expected_total=6, kill_time_ms=5000
        )
        self.assertIsNone(metrics["storm"]["recovery_secs"])
        self.assertEqual(metrics["storm"]["replaced_total"], 0)


class LegionEventParsing(unittest.TestCase):
    """The envelope contract: `legion events follow -j` NDJSON ->
    internal placement events. Fixture lines were captured from the
    documented event shape (docs/event-stream.md, src/storage/events.rs)."""

    def test_placed_event_with_count(self):
        line = json.dumps(
            {
                "ok": True,
                "data": {
                    "id": 123456,
                    "kind": "allocation.placed",
                    "level": "info",
                    "job_id": "bench-api-0",
                    "message": "Placed 4 allocation(s) for job bench-api-0",
                    "metadata_json": json.dumps(
                        {"jobConfigId": "cfg", "count": 4, "placements": []}
                    ),
                },
            }
        )
        self.assertEqual(
            parse_legion_event(line), {"job": "bench-api-0", "count": 4}
        )

    def test_non_placement_kinds_are_ignored(self):
        for kind in ["allocation.failed", "allocation.removed", "eval.completed"]:
            line = json.dumps({"ok": True, "data": {"kind": kind, "job_id": "j"}})
            self.assertIsNone(parse_legion_event(line))

    def test_garbage_and_error_envelopes_are_ignored(self):
        self.assertIsNone(parse_legion_event(""))
        self.assertIsNone(parse_legion_event("not json"))
        self.assertIsNone(
            parse_legion_event(json.dumps({"ok": False, "error": {"code": "UNAVAILABLE"}}))
        )

    def test_missing_count_defaults_to_one(self):
        line = json.dumps(
            {
                "ok": True,
                "data": {"kind": "allocation.placed", "job_id": "j", "metadata_json": "{}"},
            }
        )
        self.assertEqual(parse_legion_event(line), {"job": "j", "count": 1})


class TranslatorShapes(unittest.TestCase):
    """The translator's output contract per system, on the real
    web_fleet scenario."""

    def setUp(self):
        self.scenario = translate.load_scenario(
            os.path.join(SCENARIOS, "web_fleet.json")
        )

    def test_job_expansion_and_expected_totals(self):
        jobs = list(translate.iter_jobs(self.scenario, 1.0))
        # web_fleet: api x20 + cron x10.
        self.assertEqual(len(jobs), 30)
        total = sum(translate.expected_allocations(job_type) for _, job_type, _ in jobs)
        # api: 2 tasks x 4 = 8 each; cron: 1 x 2 = 2 each -> 20*8 + 10*2.
        self.assertEqual(total, 180)

    def test_count_multiplier_scales_jobs(self):
        jobs = list(translate.iter_jobs(self.scenario, 2.0))
        self.assertEqual(len(jobs), 60)
        # A fractional multiplier rounds up, never to zero.
        jobs = list(translate.iter_jobs(self.scenario, 0.01))
        self.assertEqual(len(jobs), 2)

    def test_legion_jobspec_shape(self):
        _, api, _ = next(iter(translate.iter_jobs(self.scenario, 1.0)))
        spec = translate.legion_jobspec("bench-api-0", api)
        self.assertEqual(spec["name"], "bench-api-0")
        self.assertEqual(len(spec["tasks"]), 2)
        task = spec["tasks"][0]
        self.assertEqual(task["runtime"], "noop")
        self.assertEqual(task["count"], 4)
        self.assertEqual(task["constraints"], {"class": "web"})
        self.assertEqual(task["resources"], {"cpu": 500, "memory": 1024})
        # Distinct groups so tasks place independently.
        self.assertNotEqual(spec["tasks"][0]["group"], spec["tasks"][1]["group"])

    def test_nomad_job_shape(self):
        _, api, _ = next(iter(translate.iter_jobs(self.scenario, 1.0)))
        hcl = translate.nomad_job("bench-api-0", api)
        self.assertIn('job "bench-api-0"', hcl)
        self.assertIn('driver = "mock"', hcl)
        self.assertIn("count = 4", hcl)
        self.assertIn('attribute = "${node.class}"', hcl)
        self.assertIn("cpu    = 500", hcl)
        self.assertEqual(hcl.count('group "'), 2)

    def test_k8s_deployment_shape(self):
        _, api, _ = next(iter(translate.iter_jobs(self.scenario, 1.0)))
        deployment = translate.k8s_deployment("bench-api-0", api, 0)
        spec = deployment["spec"]
        self.assertEqual(spec["replicas"], 4)
        pod = spec["template"]["spec"]
        self.assertEqual(pod["nodeSelector"], {"type": "kwok", "class": "web"})
        self.assertEqual(pod["tolerations"][0]["key"], "kwok.x-k8s.io/node")
        requests = pod["containers"][0]["resources"]["requests"]
        # 500 MHz at the fleet's 3000 MHz/core fingerprint -> 167m.
        self.assertEqual(requests["cpu"], "167m")
        self.assertEqual(requests["memory"], "1024Mi")


if __name__ == "__main__":
    unittest.main()
