#!/usr/bin/env python3
"""Workload driver for the AWS comparative benchmark.

Submits a translated workload (workloads/translate.py output) to one
system, watches that system's event/watch stream, and measures:

  placement throughput   placed allocations/sec over the steady window
                         (warm-up discarded)
  placement latency      submit -> placed, p50/p95/p99, weighted by the
                         allocations each placement event covered
  completeness           placed vs the manifest's expected total — a run
                         that never converged is reported, not hidden
  recovery               with --kill-time-ms (storm scenario): seconds
                         from the kill to the last replacement placement

Timestamps are DRIVER-SIDE receive times for every system — the same
clock takes submit and placed marks, so no cross-host clock skew enters
a latency, and all three systems pay the same stream-delivery latency.
That methodology choice is recorded in the campaign fairness notes.

FAIRNESS RULE — SUBMISSION PATHS ARE NOT INTERCHANGEABLE
--------------------------------------------------------
Legion has a second, much faster submission path: `legion-loadgen`, a
native binary that holds a few gRPC connections open and issues many
concurrent RunJob calls (`benchmarks/loadgen/main.rs`). Nomad and K8s
have no equivalent; they still spawn one CLI process per job, which is
the shared loop that made the three-system comparison fair.

So a number produced with `--submitter native` is comparable to another
Legion number, and to NOTHING ELSE. Any cross-system comparison must
either

  * run all three with `--submitter cli`, or
  * add equivalent native submitters for Nomad and K8s first.

The submitter used is recorded in every cell's metrics as `submitter`,
so an archive can always be checked rather than trusted. This is a
correctness property of the benchmark, not a preference — see
benchmarks/aws/README.md § Methodology.

Subcommands:
  run     drive a workload live (legion fully supported; nomad/k8s
          adapters ship for their campaign phases and are marked
          to-verify)
  replay  recompute metrics from a recorded run directory — the fixture
          path that keeps the counting math honest without a cluster
          (tests/test_driver.py, exercised in CI)

A run directory contains:
  submits.json          [{"job": name, "t_ms": submit-time}]
  driver-events.ndjson  {"t_ms":..., "job":..., "count":...} per placement
  raw-events.ndjson     untouched upstream lines (debugging)
  metrics.json          the computed result
"""

import argparse
import re
import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.request


# Default parallelism per submission path. They are different numbers because
# a "slot" costs wildly different things: on the CLI loop it is a whole OS
# process (so the useful count is bounded by the loadgen box's vCPUs), and on
# the native submitter it is one in-flight unary RPC (so the useful count is
# bounded by what keeps the manager's request handling busy). --submit-concurrency
# overrides either.
CLI_SUBMIT_CONCURRENCY = 32
# Matches legion-loadgen's own default, which was measured rather than picked:
# submission throughput peaks around here and falls off past a few hundred in
# flight, because the surplus only queues at the manager's control-lane commit.
NATIVE_SUBMIT_CONCURRENCY = 128


def now_ms():
    return int(time.monotonic() * 1000)


def monotonic_offset_ms():
    """Milliseconds to add to a UNIX-epoch stamp to put it on now_ms()'s clock.

    `legion-loadgen` runs in its own process and so cannot share this one's
    monotonic clock — Python's `time.monotonic()` and Rust's `Instant` are
    both deliberately opaque, epoch-less readings. Epoch time is the only
    clock two processes can name the same instant on, so the submitter stamps
    in epoch ms and the driver hands it this offset to rebase with.

    Both clocks tick at the same rate, so the offset is stable for the length
    of a cell; only an NTP *step* mid-cell could skew a latency, and the rig's
    hosts slew rather than step.
    """
    return now_ms() - int(time.time() * 1000)


# ---------------------------------------------------------------------
# Metric math (pure; unit-tested in tests/test_driver.py)
# ---------------------------------------------------------------------


def percentile(sorted_values, fraction):
    """Nearest-rank on a sorted list (same convention as the Rust
    benchmark runner's pctl())."""
    if not sorted_values:
        return 0.0
    idx = round((len(sorted_values) - 1) * fraction)
    return float(sorted_values[idx])


def submission_rate_series(submits, bucket_secs=60.0):
    """Jobs submitted per bucket from the first submit, in order.

    Phase 5 saw the CLI loop's rate sag from 89 to 17 jobs/sec across a
    day and nobody noticed until the archives were re-read, because
    `submission.rate_per_sec` is one average over the whole cell. A series
    shows a sag as a slope. Buckets with no submits are kept as zeros so the
    series is a timeline, not a histogram of busy minutes.
    """
    if not submits or bucket_secs <= 0:
        return []
    stamps = sorted(submits.values())
    first = stamps[0]
    width_ms = bucket_secs * 1000.0
    buckets = int((stamps[-1] - first) // width_ms) + 1
    series = [0] * buckets
    for stamp in stamps:
        series[int((stamp - first) // width_ms)] += 1
    return series


def parse_prometheus_gauge(text, name):
    """Sum of every sample of `name` in Prometheus text exposition, or None
    when the family is absent. Labelled samples are summed; `name{...}` and
    bare `name` both match, `name_total` and `name_created` do not."""
    total = None
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^([A-Za-z_:][A-Za-z0-9_:]*)(\{[^}]*\})?\s+(\S+)", line)
        if not match or match.group(1) != name:
            continue
        try:
            value = float(match.group(3))
        except ValueError:
            continue
        total = value if total is None else total + value
    return total


def compute_metrics(submits, events, expected_total, warmup_secs=0.0, kill_time_ms=None):
    """submits: {job: t_ms}; events: [{t_ms, job, count}] placements.

    The steady window opens warmup_secs after the FIRST submit and closes
    at the last placement; throughput counts only window placements.
    Latency samples are (event.t_ms - submit[job]) weighted by count and
    also exclude the warm-up.
    """
    placed_total = sum(event["count"] for event in events)
    first_submit = min(submits.values()) if submits else 0
    warmup_end = first_submit + int(warmup_secs * 1000)

    # A cell shorter than its own warm-up measures nothing: every placement
    # is excluded and the cell reports `throughput_per_sec: 0.0` alongside
    # `complete: true`, which reads as a catastrophic result rather than a
    # missing one. That is not hypothetical — the 2026-08-04 phase-3
    # campaign lost its 1,000-node throughput number exactly this way, at
    # the default 30s warm-up.
    #
    # Fall back to measuring the whole run rather than nothing, and say so
    # in the metrics: a number with `warmup_applied: false` is comparable
    # to nothing else, but it is honest and it is not zero.
    last_event = max((event["t_ms"] for event in events), default=warmup_end)
    warmup_applied = last_event > warmup_end
    if not warmup_applied:
        warmup_end = first_submit

    latencies = []
    window_placed = 0
    window_start = None
    window_end = None
    for event in events:
        if event["t_ms"] < warmup_end:
            continue
        submit = submits.get(event["job"])
        if submit is not None:
            latency = max(0, event["t_ms"] - submit)
            latencies.extend([latency] * event["count"])
        window_placed += event["count"]
        window_start = event["t_ms"] if window_start is None else min(window_start, event["t_ms"])
        window_end = event["t_ms"] if window_end is None else max(window_end, event["t_ms"])

    if window_placed > 0 and window_end > window_start:
        window_secs = (window_end - window_start) / 1000.0
        throughput = window_placed / window_secs
    elif window_placed > 0:
        # Everything landed in one tick; report the placements against a
        # 1ms floor rather than infinity.
        throughput = window_placed / 0.001
    else:
        throughput = 0.0

    latencies.sort()
    metrics = {
        "placed_total": placed_total,
        "expected_total": expected_total,
        "complete": placed_total >= expected_total,
        "steady_window_placed": window_placed,
        "steady_window_secs": round(((window_end - window_start) / 1000.0), 3)
        if window_start is not None and window_end is not None
        else 0.0,
        "throughput_per_sec": round(throughput, 2),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "latency_samples": len(latencies),
        "warmup_secs": warmup_secs,
        # False means the cell finished inside its own warm-up and the
        # numbers above cover the WHOLE run, warm-up included. Read such a
        # cell as a floor, and shorten `--warmup` before comparing it.
        "warmup_applied": warmup_applied,
    }

    # Is this cell measuring the system under test, or the driver?
    #
    # Submission is a CLI process per job from a thread pool, so it has its
    # own ceiling, and when the scheduler is faster than that ceiling the
    # cell reports the DRIVER's rate with no sign that it has done so. The
    # 2026-08-05 phase-4 campaign is the worked example: 4,167 jobs went out
    # over 114.97s (36.24 jobs/sec) and the cell reported 203.19 alloc/sec,
    # which is 36.24 x 6 allocations per job. The control plane sat at 8%
    # CPU with an empty eval queue. Three sessions of "throughput" numbers
    # were submission rates.
    #
    # The signal is the lag between the two streams: if placement finishes
    # essentially when submission does, placement was never behind, and the
    # rate is submission's. Report it rather than leaving it to be noticed.
    if submits and events:
        last_submit = max(submits.values())
        submit_span = (last_submit - first_submit) / 1000.0
        submit_rate = len(submits) / submit_span if submit_span > 0 else None
        # How long after the last job was submitted the last allocation
        # landed. Near zero means the scheduler was never the constraint.
        drain_secs = (last_event - last_submit) / 1000.0
        window = metrics["steady_window_secs"] or 0.0
        driver_bound = window > 0 and drain_secs < 0.1 * window
        metrics["submission"] = {
            "jobs": len(submits),
            "span_secs": round(submit_span, 3),
            "rate_per_sec": round(submit_rate, 2) if submit_rate else None,
            # Placement time left after the last submit. A cell whose
            # scheduler is the constraint keeps draining after submission
            # stops; one that is driver-bound finishes with it.
            "drain_secs": round(drain_secs, 3),
            # True means throughput_per_sec is a floor for the system under
            # test and an accurate measure of nothing but this driver.
            "driver_bound": driver_bound,
            # Jobs submitted per minute from the first submit. A flat series
            # is a healthy driver; a falling one is the phase-5 sag (89 ->
            # 17 jobs/sec over a day) that a single average hides.
            "per_minute": submission_rate_series(submits),
        }
        if driver_bound:
            print(
                f"WARNING: DRIVER-BOUND CELL. Submission ran at "
                f"{submit_rate:.1f} jobs/sec and the last allocation landed "
                f"{drain_secs:.1f}s after the last submit, so the scheduler "
                f"was never behind. throughput_per_sec="
                f"{metrics['throughput_per_sec']} is this driver's ceiling, "
                f"NOT the system's. Raise --submit-concurrency, use a bigger "
                f"loadgen box, or use a scenario with more allocations per "
                f"job before comparing this number to anything.",
                file=sys.stderr,
            )

    if kill_time_ms is not None:
        replacements = [event for event in events if event["t_ms"] >= kill_time_ms]
        replaced = sum(event["count"] for event in replacements)
        metrics["storm"] = {
            "kill_time_ms": kill_time_ms,
            "replaced_total": replaced,
            # Storm scenario: time to 100%-replaced.
            "recovery_secs": round((max(event["t_ms"] for event in replacements) - kill_time_ms) / 1000.0, 3)
            if replacements
            else None,
            # Failover drill: the placement gap — how long the scheduler
            # was unable to place anything after the fault.
            "placement_gap_secs": round((min(event["t_ms"] for event in replacements) - kill_time_ms) / 1000.0, 3)
            if replacements
            else None,
        }

    return metrics


def parse_legion_event(line):
    """One `legion events follow -j` NDJSON line -> internal placement
    event ({job, count}, no timestamp) or None for non-placement lines.

    The envelope is {"ok":true,"data":{...event...}};
    `allocation.placed` is emitted once per plan apply with the number of
    allocations in metadata.count (src/storage/events.rs)."""
    line = line.strip()
    if not line:
        return None
    try:
        envelope = json.loads(line)
    except json.JSONDecodeError:
        return None
    data = envelope.get("data") or {}
    if not envelope.get("ok") or data.get("kind") != "allocation.placed":
        return None
    job = data.get("job_id")
    if job is None:
        return None
    try:
        metadata = json.loads(data.get("metadata_json") or "{}")
    except json.JSONDecodeError:
        metadata = {}
    return {"job": job, "count": int(metadata.get("count", 1))}


# ---------------------------------------------------------------------
# Run recording
# ---------------------------------------------------------------------


class RunRecorder:
    """Collects submits + placement events and persists the run dir."""

    def __init__(self, out_dir):
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)
        self.submits = {}
        self.events = []
        self._lock = threading.Lock()
        self._events_file = open(os.path.join(out_dir, "driver-events.ndjson"), "w")
        self._raw_file = open(os.path.join(out_dir, "raw-events.ndjson"), "w")

    def record_submit(self, job):
        # Locked because submission is concurrent (--submit-concurrency).
        # CPython would make the bare assignment atomic anyway, but the
        # timestamp is a measurement input and this is cheap.
        with self._lock:
            self.submits[job] = now_ms()

    def record_submits(self, rows):
        """Ingest a whole batch of submit stamps taken by another process.

        The native submitter (`legion-loadgen`) records its own timestamps —
        that is the point, since the driver never sees the individual RPCs —
        and hands them back in the same `[{"job", "t_ms"}]` shape this class
        writes. They arrive already rebased onto this process's clock via
        --clock-offset-ms, so nothing here converts anything.
        """
        with self._lock:
            for row in rows:
                self.submits[row["job"]] = int(row["t_ms"])

    def record_placement(self, job, count):
        event = {"t_ms": now_ms(), "job": job, "count": count}
        with self._lock:
            self.events.append(event)
            self._events_file.write(json.dumps(event) + "\n")
            self._events_file.flush()
        return event

    def record_raw(self, line):
        with self._lock:
            self._raw_file.write(line.rstrip("\n") + "\n")

    def placed_total(self):
        with self._lock:
            return sum(event["count"] for event in self.events)

    def close(self):
        """Release the NDJSON handles without computing metrics.

        `finalize()` is the normal path and closes them itself; this exists
        for callers that only wanted the recording side (the fixture tests),
        so a run dir is never left with open handles."""
        self._events_file.close()
        self._raw_file.close()

    def finalize(self, expected_total, warmup_secs, kill_time_ms=None):
        with open(os.path.join(self.out_dir, "submits.json"), "w") as f:
            json.dump(
                [{"job": job, "t_ms": t} for job, t in sorted(self.submits.items())],
                f,
                indent=2,
            )
        self.close()
        metrics = compute_metrics(
            self.submits, self.events, expected_total, warmup_secs, kill_time_ms
        )
        with open(os.path.join(self.out_dir, "metrics.json"), "w") as f:
            json.dump(metrics, f, indent=2)
            f.write("\n")
        return metrics


# ---------------------------------------------------------------------
# System adapters
# ---------------------------------------------------------------------


def resolve_loadgen_bin(explicit, legion_bin):
    """Where the native submitter lives, or None.

    Looked up next to the `legion` binary first (that is how the rig
    installs it, and how a local `cargo build` lays it out) and then on
    PATH. Returning None is not an error here — `--submitter auto` falls
    back to the CLI loop and says so, so a box that has not been
    re-provisioned still produces a (slower, honest) cell.
    """
    def usable(path):
        return path and os.path.isfile(path) and os.access(path, os.X_OK)

    if explicit:
        # Validated rather than trusted: run-step.sh always passes the rig's
        # install path, and a box provisioned before the submitter existed
        # would otherwise die on FileNotFoundError instead of falling back.
        return explicit if usable(explicit) else None
    sibling = os.path.join(
        os.path.dirname(os.path.abspath(legion_bin)), "legion-loadgen"
    )
    if usable(sibling):
        return sibling
    return shutil.which("legion-loadgen")


class LegionAdapter:
    """Fully supported: submits via `legion-loadgen` (or `legion job run
    -j` on the CLI path), watches `legion events follow -j --kind
    allocation` NDJSON."""

    def __init__(self, manager_addr, legion_bin="legion", loadgen_bin=None):
        self.manager_addr = manager_addr
        self.env = dict(os.environ, LEGION_ADDR=manager_addr)
        self.legion_bin = legion_bin
        # None means "CLI loop only" — set by cmd_run from --submitter.
        self.loadgen_bin = loadgen_bin
        self._follower = None

    def start_watch(self, recorder):
        # Keep the follower's stderr. Discarding it cost a 10-minute cell
        # that reported 0 placements: the follower had exited, the pump
        # loop ended with it, and the driver sat in its settle loop
        # waiting for a stream that was no longer there -- with the one
        # explanation written to /dev/null.
        self._stderr_path = os.path.join(recorder.out_dir, "follower-stderr.log")
        self._stderr_file = open(self._stderr_path, "w")
        self._follower = subprocess.Popen(
            [self.legion_bin, "events", "follow", "-j", "--kind", "allocation"],
            stdout=subprocess.PIPE,
            stderr=self._stderr_file,
            env=self.env,
            text=True,
        )

        def pump():
            for line in self._follower.stdout:
                recorder.record_raw(line)
                event = parse_legion_event(line)
                if event:
                    recorder.record_placement(event["job"], event["count"])

        thread = threading.Thread(target=pump, daemon=True)
        thread.start()
        # Let the subscription reach the manager before the first submit,
        # or early placements race the stream and vanish.
        time.sleep(1.0)

    def submit_all(self, workload_dir, jobs, recorder, concurrency):
        """Submit the whole workload with the native submitter.

        Returns False when there is no submitter binary, which leaves the
        caller on the shared CLI loop. Anything else that goes wrong raises:
        a cell short of its expected count must fail, not average over the
        gap.

        LEGION-ONLY, and that asymmetry is deliberate — see the fairness rule
        in this module's docstring before comparing the resulting number to a
        Nomad or K8s cell.
        """
        if not self.loadgen_bin:
            return False

        # The submitter's own output, kept next to the run rather than in a
        # temp dir: when a cell disagrees with itself, the raw stamps are the
        # evidence. `recorder.finalize()` still writes the canonical
        # submits.json, so the run-dir format is unchanged.
        raw_path = os.path.join(recorder.out_dir, "submitter-submits.json")
        command = [
            self.loadgen_bin,
            "--workload-dir", workload_dir,
            "--out", raw_path,
            "--manager", self.manager_addr,
            "--concurrency", str(max(1, concurrency)),
            # Sampled here, immediately before the child starts, so its epoch
            # stamps land on this process's monotonic clock — the same clock
            # that takes the placement marks.
            "--clock-offset-ms", str(monotonic_offset_ms()),
        ]
        result = subprocess.run(command, capture_output=True, env=self.env, text=True)
        if result.stderr:
            sys.stderr.write(result.stderr)
        if result.returncode != 0:
            raise RuntimeError(
                f"legion-loadgen failed (exit {result.returncode}): "
                f"{result.stdout.strip()}"
            )

        with open(raw_path) as f:
            rows = json.load(f)
        if len(rows) != len(jobs):
            raise RuntimeError(
                f"legion-loadgen recorded {len(rows)} submits for a workload of "
                f"{len(jobs)} jobs; the cell cannot reach its expected count"
            )
        recorder.record_submits(rows)
        return True

    def submit(self, path, job_name):
        result = subprocess.run(
            [self.legion_bin, "job", "run", path, "-j"],
            capture_output=True,
            env=self.env,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"legion job run {job_name} failed: {result.stdout} {result.stderr}")
        reply = json.loads(result.stdout)
        if not reply.get("ok"):
            raise RuntimeError(f"legion job run {job_name} rejected: {reply}")

    def watch_alive(self):
        return self._follower is not None and self._follower.poll() is None

    def metrics_url(self):
        """The manager's Prometheus endpoint: `server.port + 2` by convention
        (docs/metrics.md), unless the run named one."""
        if getattr(self, "_metrics_url", None):
            return self._metrics_url
        from urllib.parse import urlsplit, urlunsplit

        parts = urlsplit(self.manager_addr)
        port = (parts.port or 4747) + 2
        netloc = f"{parts.hostname}:{port}"
        return urlunsplit((parts.scheme or "http", netloc, "/metrics", "", ""))

    def snapshot_counters(self, timeout=5.0):
        """The manager's own view of live allocations, for the counting
        cross-check. None when the endpoint is unreachable — the check is
        advisory and must never fail a cell on its own."""
        try:
            with urllib.request.urlopen(self.metrics_url(), timeout=timeout) as response:
                text = response.read().decode("utf-8", "replace")
        except Exception as err:  # noqa: BLE001 — advisory path
            return {"error": f"{type(err).__name__}: {err}", "url": self.metrics_url()}
        return {
            "url": self.metrics_url(),
            "allocations": parse_prometheus_gauge(text, "legion_allocations"),
            "plan_apply_applied": parse_prometheus_gauge(text, "legion_plan_apply_total"),
        }

    def stop_watch(self):
        if self._follower:
            self._follower.terminate()
        if getattr(self, "_stderr_file", None):
            self._stderr_file.close()


class NomadAdapter:
    """Nomad campaign (plan phase 3). Submits via the nomad CLI; watches
    the allocation topic of the HTTP event stream. TO VERIFY against the
    pinned Nomad release when phase 3 starts — the campaign gate is the
    same fixture-replay math either way."""

    def __init__(self, server_addr, nomad_bin="nomad"):
        self.server_addr = server_addr.rstrip("/")
        self.env = dict(os.environ, NOMAD_ADDR=self.server_addr)
        self.nomad_bin = nomad_bin
        self._stop = threading.Event()

    def start_watch(self, recorder):
        import urllib.request

        def pump():
            url = f"{self.server_addr}/v1/event/stream?topic=Allocation"
            seen = set()
            with urllib.request.urlopen(url) as stream:
                for raw in stream:
                    if self._stop.is_set():
                        return
                    line = raw.decode("utf-8", "replace").strip()
                    if not line or line == "{}":
                        continue  # heartbeat frames
                    recorder.record_raw(line)
                    try:
                        frame = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    for event in frame.get("Events", []):
                        allocation = (event.get("Payload") or {}).get("Allocation") or {}
                        alloc_id = allocation.get("ID")
                        # First sighting of an allocation == placed (the
                        # scheduler created it against a node).
                        if alloc_id and alloc_id not in seen and allocation.get("NodeID"):
                            seen.add(alloc_id)
                            recorder.record_placement(allocation.get("JobID", "?"), 1)

        thread = threading.Thread(target=pump, daemon=True)
        thread.start()
        time.sleep(1.0)

    def submit_all(self, workload_dir, jobs, recorder, concurrency):
        # No native submitter for Nomad, so the shared CLI loop it is. This
        # is the fairness baseline: giving Legion a native submitter and
        # leaving Nomad on process-per-job would compare the submitters, not
        # the schedulers. Adding one here (nomad's HTTP API from a
        # long-lived session) is what a cross-system phase needs first.
        return False

    def submit(self, path, job_name):
        result = subprocess.run(
            [self.nomad_bin, "job", "run", "-detach", path],
            capture_output=True,
            env=self.env,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"nomad job run {job_name} failed: {result.stderr}")

    def watch_alive(self):
        return not self._stop.is_set()

    def stop_watch(self):
        self._stop.set()


class K8sAdapter:
    """K8s campaign (plan phase 4). Submits manifests with kubectl;
    watches bench-labeled pods and counts a pod as placed the first time
    it carries spec.nodeName (the scheduler bound it). TO VERIFY against
    the pinned K8s release when phase 4 starts."""

    def __init__(self, kubeconfig=None, kubectl_bin="kubectl"):
        self.env = dict(os.environ)
        if kubeconfig:
            self.env["KUBECONFIG"] = kubeconfig
        self.kubectl_bin = kubectl_bin
        self._watcher = None

    def start_watch(self, recorder):
        self._watcher = subprocess.Popen(
            [
                self.kubectl_bin,
                "get",
                "pods",
                "-l",
                "bench=1",
                "--watch",
                "--output-watch-events",
                "-o",
                "json",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=self.env,
            text=True,
        )

        def pump():
            seen = set()
            decoder = json.JSONDecoder()
            buffer = ""
            for line in self._watcher.stdout:
                recorder.record_raw(line)
                buffer += line
                # kubectl pretty-prints one watch event over many lines;
                # decode greedily as objects complete.
                while buffer:
                    try:
                        obj, end = decoder.raw_decode(buffer.strip())
                    except json.JSONDecodeError:
                        break
                    buffer = buffer.strip()[end:]
                    pod = obj.get("object") or {}
                    uid = (pod.get("metadata") or {}).get("uid")
                    node = (pod.get("spec") or {}).get("nodeName")
                    job = ((pod.get("metadata") or {}).get("labels") or {}).get(
                        "bench-job", "?"
                    )
                    if uid and node and uid not in seen:
                        seen.add(uid)
                        recorder.record_placement(job, 1)

        thread = threading.Thread(target=pump, daemon=True)
        thread.start()
        time.sleep(1.0)

    def submit_all(self, workload_dir, jobs, recorder, concurrency):
        # Same as Nomad: no native submitter, so the shared CLI loop keeps
        # the comparison honest. The equivalent here would be the apiserver
        # REST API over one keep-alive session rather than kubectl per job.
        return False

    def submit(self, path, job_name):
        result = subprocess.run(
            [self.kubectl_bin, "apply", "-f", path],
            capture_output=True,
            env=self.env,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"kubectl apply {job_name} failed: {result.stderr}")

    def watch_alive(self):
        return self._watcher is not None and self._watcher.poll() is None

    def stop_watch(self):
        if self._watcher:
            self._watcher.terminate()


def make_adapter(args):
    if args.system == "legion":
        # --submitter decides whether the native path is even wired up, so
        # the adapter cannot silently take it. `cli` hands back an adapter
        # with no submitter at all, which is the switch a cross-system
        # comparison must throw.
        mode = getattr(args, "submitter", "auto")
        loadgen_bin = None
        if mode != "cli":
            loadgen_bin = resolve_loadgen_bin(
                getattr(args, "legion_loadgen_bin", None), args.legion_bin
            )
            if mode == "native" and not loadgen_bin:
                raise SystemExit(
                    "--submitter native: no legion-loadgen binary found next to "
                    f"{args.legion_bin} or on PATH. Build it with "
                    "`cargo build --release --features loadgen --bin legion-loadgen`, "
                    "or pass --legion-loadgen-bin."
                )
        return LegionAdapter(
            args.manager, legion_bin=args.legion_bin, loadgen_bin=loadgen_bin
        )
    if args.system == "nomad":
        return NomadAdapter(args.manager, nomad_bin=args.nomad_bin)
    if args.system == "k8s":
        return K8sAdapter(kubeconfig=args.kubeconfig, kubectl_bin=args.kubectl_bin)
    raise ValueError(args.system)


# ---------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------


def cmd_run(args):
    if args.submitter == "native" and args.submit_gap_ms:
        # Refused rather than silently downgraded: a run that asked for the
        # native path and got the CLI loop would archive `submitter: "cli"`
        # and leave the operator wondering why. The two flags want opposite
        # things anyway.
        raise SystemExit(
            "--submitter native and --submit-gap-ms are contradictory: pacing "
            "is implemented on the CLI loop only, because the native submitter "
            "exists to remove the submission ceiling rather than to hold it. "
            "Drop one."
        )

    with open(os.path.join(args.workload_dir, "manifest.json")) as f:
        manifest = json.load(f)
    expected_total = manifest["total_expected_allocations"]

    recorder = RunRecorder(args.out)
    adapter = make_adapter(args)
    if getattr(args, "metrics_url", None) and hasattr(adapter, "metrics_url"):
        adapter._metrics_url = args.metrics_url
    adapter.start_watch(recorder)

    # The follower counts placements by parsing one NDJSON event stream on
    # one Python thread. It has kept up so far because `allocation.placed`
    # is coalesced per plan apply, but at native-submitter rates it is the
    # next plausible floor — and a follower that silently drops events
    # reports a slower scheduler, not a broken driver. So the manager's own
    # allocation gauge is sampled before and after, and the delta is
    # compared with the follower's count at the end.
    counters_before = (
        adapter.snapshot_counters() if hasattr(adapter, "snapshot_counters") else None
    )

    print(
        f"submitting {len(manifest['jobs'])} jobs "
        f"({expected_total} expected allocations) to {args.system}",
        file=sys.stderr,
    )

    # The native path first, when the system has one and the run allows it.
    # `--submit-gap-ms` is deliberately not forwarded: pacing exists to make
    # submission SLOWER, and a paced cell has no use for a submitter built to
    # remove the ceiling — so a paced run stays on the shared loop, where the
    # gap has always been implemented.
    submitter = "cli"
    if not args.submit_gap_ms:
        native_concurrency = (
            args.submit_concurrency
            if args.submit_concurrency is not None
            else NATIVE_SUBMIT_CONCURRENCY
        )
        if adapter.submit_all(
            args.workload_dir, manifest["jobs"], recorder, native_concurrency
        ):
            submitter = "native"

    if submitter == "cli":
        cli_concurrency = (
            args.submit_concurrency
            if args.submit_concurrency is not None
            else CLI_SUBMIT_CONCURRENCY
        )

        def submit_one(job):
            # Stamp inside the worker, immediately before the call, or the
            # submit->placed latency picks up queueing in this pool.
            recorder.record_submit(job["name"])
            adapter.submit(os.path.join(args.workload_dir, job["file"]), job["name"])
            if args.submit_gap_ms:
                time.sleep(args.submit_gap_ms / 1000.0)

        # Submission has to outpace placement or the driver, not the system
        # under test, sets the measured rate. Serially it does not come
        # close: each submit is a fresh CLI process plus an RPC, which
        # measured 25ms -- about 40 jobs/sec -- and on a clean 1000-node
        # cluster Legion placed every job before the next one arrived (last
        # placement landed 0.0s after the last submit). That run measured
        # subprocess spawn, not the scheduler.
        #
        # Raising the pool only moves the ceiling; it does not remove it,
        # because the per-job cost is a whole process. This loop is now the
        # FAIRNESS BASELINE rather than the fast path: it is what Nomad and
        # K8s use, and what a cross-system comparison must force Legion onto
        # with --submitter cli.
        if cli_concurrency > 1:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=cli_concurrency
            ) as pool:
                # list() so a failed submit raises here rather than being
                # silently dropped -- a cell short of its expected count must
                # fail loudly, not average over the gap.
                list(pool.map(submit_one, manifest["jobs"]))
        else:
            for job in manifest["jobs"]:
                submit_one(job)

    if args.system == "legion" and submitter == "cli":
        # Said out loud every time, because a CLI-loop Legion cell is a floor
        # and the reader has to know before they read the throughput. What to
        # do about it differs: a deliberate `--submitter cli` run is the
        # fairness baseline and is behaving correctly, while an `auto` run
        # that landed here is a box missing its submitter.
        remedy = (
            "This is the cross-system fairness baseline, so the number is a "
            "floor by design."
            if args.submitter == "cli"
            else "No legion-loadgen binary was found — install it (or pass "
            "--legion-loadgen-bin) to measure the scheduler instead."
        )
        print(
            "note: submitting via the per-job CLI loop. Its ceiling is process "
            "spawn, so it is set by the loadgen box and not by Legion "
            "(36-103 jobs/sec on the rig's m7i loadgen; a few hundred on a "
            "fast laptop), and --submit-concurrency moves it without removing "
            "it. Check `submission.driver_bound` before reading the "
            f"throughput. {remedy}",
            file=sys.stderr,
        )

    # Two clocks, because a fixed wall-clock deadline cannot tell "the
    # cluster stopped converging" from "this cell is simply bigger".
    # A 200k-allocation saturation point needs ~10 minutes at 350
    # alloc/sec just to finish healthily, so a flat 600s would report it
    # incomplete for arithmetic reasons and the sweep would read that as
    # the scheduler's ceiling — the exact class of measurement error that
    # invalidated phase 1.
    #
    #   stall  — no placement at all for this long. THIS is "stopped
    #            converging", and it is independent of cell size.
    #   settle — an absolute cap so a pathologically slow trickle still
    #            ends the cell. Scaled from the expected allocation count
    #            when the caller leaves it at 0 (= derive it).
    settle_timeout = args.settle_timeout_secs
    if settle_timeout <= 0:
        # 20 alloc/sec is a deliberately pessimistic floor — an order of
        # magnitude under the slowest cell ever measured (86.6/sec) — so
        # the cap only ever fires on a system that is not really working.
        settle_timeout = max(600.0, expected_total / 20.0)
    print(
        f"settling: stall timeout {args.stall_timeout_secs}s, "
        f"absolute cap {settle_timeout:.0f}s ({expected_total} expected)",
        file=sys.stderr,
    )

    outcome = "complete"
    deadline = time.monotonic() + settle_timeout
    last_progress = time.monotonic()
    last_placed = recorder.placed_total()
    while time.monotonic() < deadline:
        placed = recorder.placed_total()
        if placed >= expected_total:
            break
        if placed > last_placed:
            last_placed = placed
            last_progress = time.monotonic()
        elif (
            args.stall_timeout_secs > 0
            and time.monotonic() - last_progress > args.stall_timeout_secs
        ):
            outcome = "stalled"
            print(
                f"STALLED: no placement for {args.stall_timeout_secs}s at "
                f"{placed}/{expected_total} — the cluster stopped converging; "
                f"metrics record the shortfall",
                file=sys.stderr,
            )
            break
        # A dead watcher is not a slow cluster, and waiting out the full
        # settle timeout to report zero placements hides which one it
        # was. Fail as soon as the stream is gone, and say where its
        # stderr went.
        if not adapter.watch_alive():
            outcome = "watcher_died"
            print(
                f"ERROR: the event watcher exited mid-run after "
                f"{recorder.placed_total()}/{expected_total} placements. "
                f"The cell measured nothing; see {args.out}/follower-stderr.log",
                file=sys.stderr,
            )
            break
        time.sleep(0.25)
    else:
        outcome = "timeout"
        print(
            f"WARNING: settle cap reached after {settle_timeout:.0f}s "
            f"({recorder.placed_total()}/{expected_total} placed) — "
            f"metrics record the shortfall",
            file=sys.stderr,
        )

    adapter.stop_watch()
    metrics = recorder.finalize(expected_total, args.warmup_secs)
    # Why the cell ended, in the archive. An incomplete cell that stalled
    # is a scheduler result; one that hit the cap is a rig result; and
    # they are indistinguishable from `complete: false` alone.
    metrics["outcome"] = outcome
    metrics["settle_timeout_secs"] = round(settle_timeout, 1)
    # Which submission path produced this cell. Load-bearing for the fairness
    # rule at the top of this file: `native` is Legion-only, so a `native`
    # cell is comparable to another Legion cell and to nothing else. Recorded
    # per cell so an archive can be checked rather than trusted.
    metrics["submitter"] = submitter
    if counters_before is not None:
        metrics["cross_check"] = cross_check_counts(
            counters_before, adapter.snapshot_counters, recorder.placed_total()
        )
        if metrics["cross_check"].get("agree") is False:
            print(
                "WARNING: COUNTING DISAGREEMENT. The event follower saw "
                f"{metrics['cross_check']['follower_placed']} placements but the "
                "manager's legion_allocations gauge moved by "
                f"{metrics['cross_check']['manager_allocations_delta']}. Either the "
                "follower dropped events (the driver, not the scheduler, set "
                "this cell's throughput) or the fleet carried allocations from "
                "a previous cell. Read throughput_per_sec as suspect.",
                file=sys.stderr,
            )
    print(json.dumps(metrics, indent=2))
    return 0 if metrics["complete"] else 1


def cross_check_counts(before, snapshot, follower_placed, settle_secs=6.0, poll_secs=1.0):
    """Compare the follower's placement count with the manager's gauge delta.

    The gauge refreshes on a periodic tick, so the final sample is re-taken
    for up to `settle_secs` while it is still moving; the comparison uses
    the last reading. Absent or unreachable counters produce
    `available: false` rather than a verdict.
    """
    if not before or before.get("allocations") is None:
        return {"available": False, "reason": before.get("error") if before else "no snapshot"}
    after = snapshot()
    deadline = time.monotonic() + settle_secs
    while after.get("allocations") is not None and time.monotonic() < deadline:
        delta = after["allocations"] - before["allocations"]
        if delta >= follower_placed:
            break
        time.sleep(poll_secs)
        again = snapshot()
        if again.get("allocations") is None or again["allocations"] == after["allocations"]:
            after = again if again.get("allocations") is not None else after
            break
        after = again
    if after.get("allocations") is None:
        return {"available": False, "reason": after.get("error", "gauge absent")}
    delta = int(round(after["allocations"] - before["allocations"]))
    return {
        "available": True,
        "metrics_url": after.get("url"),
        "follower_placed": follower_placed,
        "manager_allocations_before": before["allocations"],
        "manager_allocations_after": after["allocations"],
        "manager_allocations_delta": delta,
        "agree": delta == follower_placed,
    }


def cmd_watch(args):
    """Fault-injection observation (storm, failover, upgrade drills):
    watch placements WITHOUT submitting, run the fault command mid-watch,
    and measure recovery/placement-gap around it — all timestamps in this
    one process's clock, so the gap math is skew-free."""
    recorder = RunRecorder(args.out)
    adapter = make_adapter(args)
    adapter.start_watch(recorder)

    print(f"watching for {args.exec_after_secs}s before the fault", file=sys.stderr)
    time.sleep(args.exec_after_secs)

    print(f"injecting fault: {args.exec_cmd}", file=sys.stderr)
    kill_time = now_ms()
    fault = subprocess.run(args.exec_cmd, shell=True, capture_output=True, text=True)
    if fault.returncode != 0:
        print(f"WARNING: fault command exited {fault.returncode}: {fault.stderr}", file=sys.stderr)
    with open(os.path.join(args.out, "fault.json"), "w") as f:
        json.dump({"cmd": args.exec_cmd, "kill_time_ms": kill_time}, f, indent=2)

    deadline = time.monotonic() + args.settle_timeout_secs
    while time.monotonic() < deadline:
        replaced = sum(
            event["count"] for event in recorder.events if event["t_ms"] >= kill_time
        )
        if args.expected_replacements and replaced >= args.expected_replacements:
            break
        time.sleep(0.5)

    adapter.stop_watch()
    metrics = recorder.finalize(
        args.expected_replacements or 0, warmup_secs=0.0, kill_time_ms=kill_time
    )
    print(json.dumps(metrics, indent=2))
    storm = metrics.get("storm") or {}
    if args.expected_replacements:
        return 0 if storm.get("replaced_total", 0) >= args.expected_replacements else 1
    return 0


def cmd_replay(args):
    with open(os.path.join(args.run, "submits.json")) as f:
        submits = {row["job"]: row["t_ms"] for row in json.load(f)}
    events = []
    with open(os.path.join(args.run, "driver-events.ndjson")) as f:
        for line in f:
            if line.strip():
                events.append(json.loads(line))
    metrics = compute_metrics(
        submits, events, args.expected_total, args.warmup_secs, args.kill_time_ms
    )
    print(json.dumps(metrics, indent=2))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="drive a workload against a live system")
    run.add_argument("--system", required=True, choices=["legion", "nomad", "k8s"])
    run.add_argument("--workload-dir", required=True, help="translate.py output dir")
    run.add_argument("--out", required=True, help="run directory to write")
    run.add_argument("--manager", default="http://127.0.0.1:4747", help="manager/server address")
    run.add_argument("--warmup-secs", type=float, default=5.0)
    run.add_argument(
        "--settle-timeout-secs",
        type=float,
        default=0.0,
        help="Absolute cap on the settle wait. 0 (the default) derives it "
        "from the expected allocation count at a pessimistic 20/sec floor, "
        "so a large saturation cell is not cut off for arithmetic reasons.",
    )
    run.add_argument(
        "--stall-timeout-secs",
        type=float,
        default=120.0,
        help="End the cell when no placement has landed for this long. This "
        "is the real 'stopped converging' signal, and unlike the cap it does "
        "not move with cell size. 0 disables it, leaving only the cap.",
    )
    run.add_argument(
        "--submit-gap-ms",
        type=int,
        default=0,
        help="Sleep this long after each submit. Pacing only makes sense on "
        "the CLI loop, so any non-zero value keeps the run on that path.",
    )
    run.add_argument(
        "--submit-concurrency",
        type=int,
        default=None,
        help="Jobs submitted in parallel. Means the same thing on both paths "
        f"but defaults differently: {CLI_SUBMIT_CONCURRENCY} on the CLI loop, "
        f"where each slot is a whole process, and {NATIVE_SUBMIT_CONCURRENCY} "
        "on the native submitter, where each slot is one in-flight RPC. 1 "
        "restores the serial loop. On the CLI path this is the setting that "
        "decides whether a cell measures the scheduler or the driver: at an "
        "old default of 4 on a 2-vCPU loadgen box, submission ceilinged at "
        "~36 jobs/sec and phase 4 reported that as Legion throughput while "
        "the control plane idled at 8% CPU. Check "
        "`submission.driver_bound` in the output before trusting a number.",
    )
    run.add_argument(
        "--submitter",
        choices=["auto", "native", "cli"],
        default="auto",
        help="How Legion jobs are submitted. `native` uses legion-loadgen, a "
        "long-lived process holding a few gRPC connections open — the only "
        "path fast enough to leave the scheduler as the constraint. `cli` "
        "forces the per-job CLI loop that Nomad and K8s use, and is REQUIRED "
        "for any cross-system comparison (see this file's fairness rule). "
        "`auto` (default) takes native when the binary is present and falls "
        "back to cli with a warning. No effect on nomad/k8s, which have no "
        "native submitter. The choice is archived as `submitter` in metrics.",
    )
    run.add_argument(
        "--legion-loadgen-bin",
        default=None,
        help="Path to legion-loadgen. Default: next to --legion-bin, then PATH.",
    )
    run.add_argument("--legion-bin", default="legion")
    run.add_argument("--nomad-bin", default="nomad")
    run.add_argument("--kubectl-bin", default="kubectl")
    run.add_argument("--kubeconfig", default=None)
    run.add_argument(
        "--metrics-url",
        default=None,
        help="The manager's Prometheus endpoint for the end-of-cell counting "
        "cross-check (legion only). Default: --manager's host on server.port "
        "+ 2, per docs/metrics.md. The check is advisory: an unreachable "
        "endpoint records `cross_check.available: false` and nothing else.",
    )
    run.set_defaults(func=cmd_run)

    watch = sub.add_parser(
        "watch", help="observe placements around an injected fault (no submission)"
    )
    watch.add_argument("--system", required=True, choices=["legion", "nomad", "k8s"])
    watch.add_argument("--out", required=True)
    watch.add_argument("--manager", default="http://127.0.0.1:4747")
    watch.add_argument("--exec-cmd", required=True, help="fault command, run via the shell")
    watch.add_argument("--exec-after-secs", type=float, default=10.0)
    watch.add_argument(
        "--expected-replacements",
        type=int,
        default=0,
        help="stop once this many placements arrive after the fault (0 = wait out the timeout)",
    )
    watch.add_argument("--settle-timeout-secs", type=float, default=600.0)
    watch.add_argument("--legion-bin", default="legion")
    watch.add_argument("--nomad-bin", default="nomad")
    watch.add_argument("--kubectl-bin", default="kubectl")
    watch.add_argument("--kubeconfig", default=None)
    watch.set_defaults(func=cmd_watch)

    replay = sub.add_parser("replay", help="recompute metrics from a recorded run dir")
    replay.add_argument("--run", required=True)
    replay.add_argument("--expected-total", type=int, required=True)
    replay.add_argument("--warmup-secs", type=float, default=0.0)
    replay.add_argument("--kill-time-ms", type=int, default=None)
    replay.set_defaults(func=cmd_replay)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
