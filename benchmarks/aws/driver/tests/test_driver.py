#!/usr/bin/env python3
"""Fixture tests for the driver's counting and latency math — the
"broken driver never burns cluster-hours" gate from the AWS benchmark
plan's testing section. Stdlib unittest only; run with:

    python3 -m unittest discover benchmarks/aws/driver/tests

(scripts/local-smoke.sh runs this before anything else, and CI runs it
on benchmark-file changes.)
"""

import argparse
import json
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "workloads"))

from driver import (  # noqa: E402
    follower_lag_summary,
    K8sAdapter,
    LegionAdapter,
    NomadAdapter,
    RunRecorder,
    compute_metrics,
    cross_check_counts,
    http_submit_all,
    iter_stream_lines,
    make_adapter,
    monotonic_offset_ms,
    parse_legion_event,
    parse_prometheus_gauge,
    percentile,
    resolve_loadgen_bin,
    submission_rate_series,
)
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


class SubmissionRateSeries(unittest.TestCase):
    def test_series_is_a_timeline_with_empty_buckets_kept(self):
        submits = {"a": 0, "b": 1_000, "c": 59_999, "d": 180_000}
        self.assertEqual(submission_rate_series(submits), [3, 0, 0, 1])

    def test_series_rides_the_submission_block(self):
        submits = {f"j{i}": i * 30_000 for i in range(6)}  # one every 30s, 2.5 min
        events = [{"t_ms": 30_000 * i + 500, "job": f"j{i}", "count": 1} for i in range(6)]
        metrics = compute_metrics(submits, events, 6, warmup_secs=0.0)
        self.assertEqual(metrics["submission"]["per_minute"], [2, 2, 2])

    def test_empty(self):
        self.assertEqual(submission_rate_series({}), [])


class PrometheusGaugeParsing(unittest.TestCase):
    TEXT = (
        "# HELP legion_allocations live allocations\n"
        "# TYPE legion_allocations gauge\n"
        "legion_allocations 25002\n"
        "legion_allocations_created 1.7e9\n"
        'legion_plan_apply_total{outcome="applied"} 2897\n'
        'legion_plan_apply_total{outcome="fallback"} 2104\n'
        "legion_nodes{status=\"active\"} 5000\n"
    )

    def test_bare_gauge(self):
        self.assertEqual(parse_prometheus_gauge(self.TEXT, "legion_allocations"), 25002.0)

    def test_labelled_samples_are_summed(self):
        self.assertEqual(parse_prometheus_gauge(self.TEXT, "legion_plan_apply_total"), 5001.0)

    def test_absent_family_is_none(self):
        self.assertIsNone(parse_prometheus_gauge(self.TEXT, "legion_evals"))


class CountingCrossCheck(unittest.TestCase):
    def test_agreement(self):
        before = {"allocations": 100.0}
        result = cross_check_counts(
            before, lambda: {"allocations": 160.0, "url": "u"}, 60, settle_secs=0.0
        )
        self.assertTrue(result["available"])
        self.assertTrue(result["agree"])
        self.assertEqual(result["manager_allocations_delta"], 60)

    def test_disagreement_is_reported_not_raised(self):
        before = {"allocations": 100.0}
        result = cross_check_counts(
            before, lambda: {"allocations": 150.0}, 60, settle_secs=0.0
        )
        self.assertFalse(result["agree"])

    def test_gauge_that_is_still_moving_is_re_sampled(self):
        readings = iter([{"allocations": 130.0}, {"allocations": 160.0}])
        before = {"allocations": 100.0}
        result = cross_check_counts(
            before, lambda: next(readings), 60, settle_secs=5.0, poll_secs=0.0
        )
        self.assertTrue(result["agree"])

    def test_unreachable_endpoint_is_advisory(self):
        result = cross_check_counts({"error": "boom"}, lambda: {}, 60)
        self.assertEqual(result, {"available": False, "reason": "boom"})
        result = cross_check_counts({"allocations": 1.0}, lambda: {"error": "x"}, 5, settle_secs=0.0)
        self.assertFalse(result["available"])


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
        # "mock_driver" is the name Nomad's mock plugin registers under, and
        # the one nodesim advertises. This assertion used to say "mock",
        # which no node offers, so every Nomad job would have been
        # infeasible.
        self.assertIn('driver = "mock_driver"', hcl)
        self.assertIn("count = 4", hcl)
        self.assertIn('attribute = "${node.class}"', hcl)
        self.assertIn("cpu    = 500", hcl)
        self.assertEqual(hcl.count('group "'), 2)

    def test_nomad_api_job_matches_the_hcl(self):
        _, api, _ = next(iter(translate.iter_jobs(self.scenario, 1.0)))
        job = translate.nomad_api_job("bench-api-0", api)["Job"]
        self.assertEqual(job["ID"], "bench-api-0")
        self.assertEqual(len(job["TaskGroups"]), 2)
        group = job["TaskGroups"][0]
        self.assertEqual(group["Count"], 4)
        self.assertEqual(group["Tasks"][0]["Driver"], "mock_driver")
        self.assertEqual(group["Tasks"][0]["Resources"], {"CPU": 500, "MemoryMB": 1024})
        self.assertEqual(
            group["Constraints"],
            [{"LTarget": "${node.class}", "RTarget": "web", "Operand": "="}],
        )

    def test_nomad_refuses_constraints_nodesim_cannot_satisfy(self):
        with self.assertRaises(ValueError):
            translate.nomad_job("j", {"tasks_per_job": 1, "task_count_per_task": 1,
                                      "constraints": {"gpu": "a100"}})

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


class NativeSubmitterWiring(unittest.TestCase):
    """The parts of the fast submission path that are pure logic, so a
    mistake in them is caught here rather than on a cluster: binary
    discovery, the cross-process clock rebase, and the fairness rule that
    keeps Nomad/K8s on the shared CLI loop."""

    def test_loadgen_is_found_next_to_the_legion_binary(self):
        with tempfile.TemporaryDirectory() as tmp:
            legion = os.path.join(tmp, "legion")
            loadgen = os.path.join(tmp, "legion-loadgen")
            for path in (legion, loadgen):
                with open(path, "w") as f:
                    f.write("#!/bin/sh\n")
                os.chmod(path, 0o755)
            self.assertEqual(resolve_loadgen_bin(None, legion), loadgen)

    def test_a_missing_explicit_loadgen_path_falls_back_rather_than_crashing(self):
        # run-step.sh always passes the rig's install path, so a loadgen box
        # provisioned before the submitter existed must degrade to the CLI
        # loop instead of dying on FileNotFoundError mid-cell.
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(
                resolve_loadgen_bin(os.path.join(tmp, "nope"), "/bin/echo")
            )

    def test_cli_mode_puts_every_system_on_the_shared_loop(self):
        # The fairness rule, as an assertion: `--submitter cli` must force
        # all three systems onto the process-per-job loop. If either
        # competitor ever took its HTTP path under `cli`, a "CLI baseline"
        # table would silently compare two different submitters.
        args = argparse.Namespace(
            system="nomad", manager="http://127.0.0.1:4646", nomad_bin="nomad",
            kubeconfig=None, kubectl_bin="kubectl", submitter="cli",
        )
        self.assertFalse(make_adapter(args).submit_all("dir", [], None, 256))
        args.system = "k8s"
        self.assertFalse(make_adapter(args).submit_all("dir", [], None, 256))

    def test_competitors_take_their_native_path_by_default(self):
        args = argparse.Namespace(
            system="nomad", manager="http://127.0.0.1:4646", nomad_bin="nomad",
            kubeconfig=None, kubectl_bin="kubectl", submitter="auto",
        )
        self.assertTrue(make_adapter(args).native)
        args.system = "k8s"
        self.assertTrue(make_adapter(args).native)

    def test_stream_lines_survive_frames_split_across_chunks(self):
        # Watch frames arrive in arbitrary chunk boundaries; a frame split
        # across two reads must come out whole, and only a real end of body
        # (read1 -> b"") may end the stream.
        class Chunks:
            def __init__(self, parts):
                self.parts = list(parts)

            def read1(self, _n):
                return self.parts.pop(0) if self.parts else b""

        lines = list(iter_stream_lines(Chunks([b'{"a":1}\n{"b"', b':2}\n', b'{"c":3}'])))
        self.assertEqual(lines, [b'{"a":1}', b'{"b":2}', b'{"c":3}'])

    def test_http_submitter_retries_server_errors_and_counts_them(self):
        statuses = [500, 503, 200]

        class Response:
            def __init__(self, status):
                self.status = status

            def read(self):
                return b"leadership lost while committing log"

        class Conn:
            def request(self, *args, **kwargs):
                pass

            def getresponse(self):
                return Response(statuses.pop(0))

        with tempfile.TemporaryDirectory() as tmp:
            recorder = RunRecorder(tmp)
            try:
                retries = http_submit_all(
                    [{"name": "bench-a-0"}],
                    lambda job: [("POST", "/v1/jobs", b"{}")],
                    Conn,
                    recorder,
                    1,
                )
            finally:
                recorder.close()
        self.assertEqual(retries, 2)
        self.assertEqual(statuses, [])

    def test_http_submitter_stamps_every_job_and_sends_every_request(self):
        sent = []

        class FakeResponse:
            status = 200

            def read(self):
                return b"{}"

        class FakeConn:
            def request(self, method, path, body=None, headers=None):
                sent.append((method, path, body))

            def getresponse(self):
                return FakeResponse()

        with tempfile.TemporaryDirectory() as tmp:
            recorder = RunRecorder(tmp)
            try:
                jobs = [{"name": f"bench-a-{i}"} for i in range(5)]
                http_submit_all(
                    jobs,
                    lambda job: [("POST", "/x", job["name"].encode())] * 2,
                    FakeConn,
                    recorder,
                    3,
                )
                self.assertEqual(len(sent), 10)
                self.assertEqual(set(recorder.submits), {j["name"] for j in jobs})
            finally:
                recorder.close()

    def test_legion_without_a_submitter_binary_falls_back(self):
        adapter = LegionAdapter("http://127.0.0.1:4747", loadgen_bin=None)
        self.assertFalse(adapter.submit_all("dir", [], None, 256))

    def test_batch_submits_land_on_the_placement_clock(self):
        # The submitter stamps in epoch ms and the driver adds
        # `monotonic - epoch`; the result has to be comparable to the
        # placement marks, or every latency in the cell is nonsense.
        with tempfile.TemporaryDirectory() as tmp:
            recorder = RunRecorder(tmp)
            try:
                offset = monotonic_offset_ms()
                epoch_now = int(time.time() * 1000)
                recorder.record_submits(
                    [{"job": "bench-a-0", "t_ms": epoch_now + offset}]
                )
                placed = recorder.record_placement("bench-a-0", 1)
            finally:
                recorder.close()

            latency = placed["t_ms"] - recorder.submits["bench-a-0"]
            # Same instant, two clocks, one process: a couple of ms of
            # sampling slop is fine; 55 years (an unrebased epoch stamp) is
            # not, and neither is a negative (a sign error).
            self.assertGreaterEqual(latency, 0)
            self.assertLess(latency, 5000)

    def test_batch_submits_feed_the_metrics_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            recorder = RunRecorder(tmp)
            try:
                recorder.record_submits(
                    [{"job": "a", "t_ms": 1000}, {"job": "b", "t_ms": 1200}]
                )
            finally:
                recorder.close()
            events = [
                {"t_ms": 1500, "job": "a", "count": 2},
                {"t_ms": 2400, "job": "b", "count": 2},
            ]
            metrics = compute_metrics(recorder.submits, events, expected_total=4)
            self.assertTrue(metrics["complete"])
            # Latencies 500ms (job a) and 1200ms (job b), two samples each;
            # nearest-rank p50 over [500, 500, 1200, 1200] is index 2.
            self.assertEqual(metrics["latency_samples"], 4)
            self.assertEqual(metrics["p50_ms"], 1200.0)
            # The last allocation landed 1.2s after the last submit over a
            # 0.9s window, so the scheduler — not submission — was behind.
            self.assertFalse(metrics["submission"]["driver_bound"])


if __name__ == "__main__":
    unittest.main()


class FollowerLagSummaryTest(unittest.TestCase):
    def test_empty_is_none(self):
        self.assertIsNone(follower_lag_summary([]))
        self.assertIsNone(follower_lag_summary(None))

    def test_percentiles(self):
        summary = follower_lag_summary([5, 1, 3, 2, 4])
        self.assertEqual(summary["samples"], 5)
        self.assertEqual(summary["p50_ms"], 3.0)
        self.assertEqual(summary["max_ms"], 5.0)


class ParseLegionEventCamelCaseTest(unittest.TestCase):
    def test_camel_case_envelope(self):
        line = json.dumps({"ok": True, "data": {
            "kind": "allocation.placed", "jobId": "bench-api-1",
            "metadataJson": json.dumps({"count": 6})}})
        self.assertEqual(parse_legion_event(line), {"job": "bench-api-1", "count": 6})
