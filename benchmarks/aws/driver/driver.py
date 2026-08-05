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
import concurrent.futures
import json
import os
import subprocess
import sys
import threading
import time


def now_ms():
    return int(time.monotonic() * 1000)


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

    def finalize(self, expected_total, warmup_secs, kill_time_ms=None):
        with open(os.path.join(self.out_dir, "submits.json"), "w") as f:
            json.dump(
                [{"job": job, "t_ms": t} for job, t in sorted(self.submits.items())],
                f,
                indent=2,
            )
        self._events_file.close()
        self._raw_file.close()
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


class LegionAdapter:
    """Fully supported: submits via `legion job run -j`, watches
    `legion events follow -j --kind allocation` NDJSON."""

    def __init__(self, manager_addr, legion_bin="legion"):
        self.env = dict(os.environ, LEGION_ADDR=manager_addr)
        self.legion_bin = legion_bin
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
        return LegionAdapter(args.manager, legion_bin=args.legion_bin)
    if args.system == "nomad":
        return NomadAdapter(args.manager, nomad_bin=args.nomad_bin)
    if args.system == "k8s":
        return K8sAdapter(kubeconfig=args.kubeconfig, kubectl_bin=args.kubectl_bin)
    raise ValueError(args.system)


# ---------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------


def cmd_run(args):
    with open(os.path.join(args.workload_dir, "manifest.json")) as f:
        manifest = json.load(f)
    expected_total = manifest["total_expected_allocations"]

    recorder = RunRecorder(args.out)
    adapter = make_adapter(args)
    adapter.start_watch(recorder)

    print(
        f"submitting {len(manifest['jobs'])} jobs "
        f"({expected_total} expected allocations) to {args.system}",
        file=sys.stderr,
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
    # subprocess spawn, not the scheduler. The same loop feeds the Nomad
    # and K8s adapters, so lifting it lifts the ceiling for all three
    # equally and the comparison stays fair.
    if args.submit_concurrency > 1:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.submit_concurrency
        ) as pool:
            # list() so a failed submit raises here rather than being
            # silently dropped -- a cell short of its expected count must
            # fail loudly, not average over the gap.
            list(pool.map(submit_one, manifest["jobs"]))
    else:
        for job in manifest["jobs"]:
            submit_one(job)

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
    print(json.dumps(metrics, indent=2))
    return 0 if metrics["complete"] else 1


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
    run.add_argument("--submit-gap-ms", type=int, default=0)
    run.add_argument(
        "--submit-concurrency",
        type=int,
        default=32,
        help="Jobs submitted in parallel. 1 restores the old serial loop, "
        "which measured the driver (~40 jobs/sec) rather than the system. "
        "Each slot is a whole CLI process, so this is the setting that "
        "decides whether a cell measures the scheduler or the driver: at "
        "the old default of 4 on a 2-vCPU loadgen box, submission ceilinged "
        "at ~36 jobs/sec and phase 4 reported that as Legion throughput "
        "while the control plane idled at 8% CPU. Size it to the loadgen "
        "box (the rig now provisions 16 vCPU) and check "
        "`submission.driver_bound` in the output before trusting a number.",
    )
    run.add_argument("--legion-bin", default="legion")
    run.add_argument("--nomad-bin", default="nomad")
    run.add_argument("--kubectl-bin", default="kubectl")
    run.add_argument("--kubeconfig", default=None)
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
