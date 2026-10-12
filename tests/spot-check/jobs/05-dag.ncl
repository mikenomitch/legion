# A data pipeline DAG over the public airport-codes dataset (~9 MB CSV):
#
#   fetch ──► validate ──┬─► by-country ─┐
#     │                  ├─► by-type ────┤
#     │                  ├─► elevation ──┼─► report ──► serve (HTTP :8085)
#     │                  └─► flaky ──────┘
#     └─ progress (sidecar, starts with fetch)
#
# Exercises: an https artifact, batch steps gated on `success`, fan-out and
# fan-in, exec and container steps in one DAG, a step that fails once and is
# retried by its restart policy, a sidecar, a host volume on the EBS data disk
# shared by every step, and a service that starts only when the batch work is
# done. All tasks share one group, so they land on one node with the volume.
#
#   legion job run tests/spot-check/jobs/05-dag.ncl --var run=1
#   legion job status airports-dag               # watch the stages complete
#   legion job logs airports-dag -f
#   curl http://<worker-public-ip>:8085/report.html
#
# Re-run from scratch with a new --var run=N (each run gets its own dir).
# Container steps need any image with python3. On macOS, pass a locally
# loaded one: --var python_image=spot-check-agent:dev
let params =
  {
    run | String | default = "1",
    python_image | String | default = "public.ecr.aws/docker/library/python:3.12-alpine",
  }
  & vars
in

# Every Python step starts by finding this run's directory on the volume.
# LEGION_VOLUME_PIPELINE is the host path for exec steps and the mount path
# for container steps, so the same code works in both.
let prelude = m%"
import csv, json, os, pathlib, sys, time
RUN = pathlib.Path(os.environ["LEGION_VOLUME_PIPELINE"]) / ("run-" + os.environ["RUN"])
RUN.mkdir(parents=True, exist_ok=True)
def log(msg):
    print(f"[{os.environ['LEGION_TASK_NAME']}] {msg}", flush=True)
def rows():
    with open(RUN / "clean.csv", newline="") as f:
        yield from csv.DictReader(f)
"%
in

# `default` priority lets a task override any of these without a merge
# conflict.
let shared = {
  group = "pipeline",
  volumes = [{ name = "pipeline", driver = "host", params = { name = "pipeline" }, path = "/work" }],
  config.env.RUN = params.run,
  resources | default = { cpu = 200, memory = 256 },
}
in

let after = fun deps => std.array.map (fun dep => { task = dep, condition = "success" }) deps in

let exec_step = fun step_name deps code =>
  shared
  & {
    name = step_name,
    runtime = "exec",
    lifecycle = "batch",
    dependsOn = after deps,
    config = { command = ["python3", "-c", prelude ++ "\n" ++ code] },
  }
in

let container_step = fun step_name deps code =>
  shared
  & {
    name = step_name,
    runtime = "container",
    lifecycle = "batch",
    dependsOn = after deps,
    config = {
      image = params.python_image,
      command = ["python3", "-c", prelude ++ "\n" ++ code],
    },
  }
in

{
  name = "airports-dag",
  tasks = [
    shared
    & {
      name = "fetch",
      runtime = "exec",
      lifecycle = "batch",
      artifacts = [
        {
          name = "airports",
          source = "https://raw.githubusercontent.com/datasets/airport-codes/main/data/airport-codes.csv",
          unpack = "file",
        },
      ],
      config = {
        command = [
          "python3",
          "-c",
          prelude
          ++ "\n"
          ++ m%"
            import shutil
            shutil.copy(os.environ["LEGION_ARTIFACT_AIRPORTS"], RUN / "raw.csv")
            n = sum(1 for _ in open(RUN / "raw.csv")) - 1
            log(f"fetched {n} airports into {RUN}")
          "%,
        ],
      },
    },

    shared
    & {
      name = "progress",
      runtime = "exec",
      lifecycle = "sidecar",
      dependsOn = [{ task = "fetch", condition = "start" }],
      config = {
        command = [
          "sh",
          "-c",
          "while true; do echo \"[progress] $(ls \"$LEGION_VOLUME_PIPELINE/run-$RUN\" 2>/dev/null | tr '\\n' ' ')\"; sleep 5; done",
        ],
      },
      resources = { cpu = 50, memory = 32 },
    },

    container_step "validate" ["fetch"] m%"
      kept = dropped = 0
      with open(RUN / "raw.csv", newline="") as src, open(RUN / "clean.csv", "w", newline="") as dst:
          reader = csv.DictReader(src)
          writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
          writer.writeheader()
          for row in reader:
              if row["type"] == "closed" or not row["elevation_ft"] or not row["iso_country"]:
                  dropped += 1
                  continue
              writer.writerow(row)
              kept += 1
      log(f"kept {kept}, dropped {dropped} (closed or missing data)")
      if kept < 1000:
          sys.exit(f"only {kept} usable rows; refusing to continue")
    "%,

    exec_step "by-country" ["validate"] m%"
      counts = {}
      for row in rows():
          counts[row["iso_country"]] = counts.get(row["iso_country"], 0) + 1
      top = sorted(counts.items(), key=lambda kv: -kv[1])[:15]
      json.dump(top, open(RUN / "by_country.json", "w"))
      log(f"top country: {top[0]}")
    "%,

    container_step "by-type" ["validate"] m%"
      counts = {}
      for row in rows():
          counts[row["type"]] = counts.get(row["type"], 0) + 1
      json.dump(sorted(counts.items(), key=lambda kv: -kv[1]), open(RUN / "by_type.json", "w"))
      log(f"{len(counts)} airport types")
    "%,

    exec_step "elevation" ["validate"] m%"
      ranked = sorted(rows(), key=lambda r: -float(r["elevation_ft"]))
      top = [{"name": r["name"], "country": r["iso_country"], "ft": r["elevation_ft"]} for r in ranked[:10]]
      json.dump(top, open(RUN / "elevation.json", "w"))
      log(f"highest: {top[0]['name']} at {top[0]['ft']} ft")
    "%,

    # Fails on its first attempt, then succeeds: the restart policy, not the
    # DAG, has to recover it, and `report` must wait for the retry.
    exec_step "flaky" ["validate"] m%"
      marker = RUN / ".flaky-attempts"
      attempts = int(marker.read_text()) + 1 if marker.exists() else 1
      marker.write_text(str(attempts))
      if attempts == 1:
          log("attempt 1: failing on purpose")
          sys.exit(3)
      json.dump({"attempts": attempts}, open(RUN / "flaky.json", "w"))
      log(f"succeeded on attempt {attempts}")
    "%
    & { restart = { policy = "onFailure", delay = "2s" } },

    exec_step "report" ["by-country", "by-type", "elevation", "flaky"] m%"
      load = lambda name: json.load(open(RUN / name))
      countries, types, high, flaky = load("by_country.json"), load("by_type.json"), load("elevation.json"), load("flaky.json")
      li = lambda items: "".join(f"<li>{a}: {b}</li>" for a, b in items)
      html = f"""<!doctype html><meta charset=utf-8><title>Airports report, run {os.environ['RUN']}</title>
      <style>body{{font:15px system-ui;max-width:44rem;margin:2rem auto}}</style>
      <h1>Airports report, run {os.environ['RUN']}</h1>
      <p>Built by the Legion DAG at {time.strftime('%Y-%m-%d %H:%M:%S')}. The flaky step took {flaky['attempts']} attempts.</p>
      <h2>Most airports by country</h2><ol>{li(countries)}</ol>
      <h2>By type</h2><ul>{li(types)}</ul>
      <h2>Highest</h2><ol>{li((h['name'] + ' (' + h['country'] + ')', h['ft'] + ' ft') for h in high)}</ol>"""
      (RUN / "report.html").write_text(html)
      json.dump({"countries": len(countries), "flaky_attempts": flaky["attempts"]}, open(RUN / "summary.json", "w"))
      log("report.html written")
    "%,

    shared
    & {
      name = "serve",
      runtime = "exec",
      lifecycle = "service",
      dependsOn = after ["report"],
      config = {
        command = [
          "sh",
          "-c",
          "exec python3 -m http.server \"$LEGION_PORT_HTTP\" --directory \"$LEGION_VOLUME_PIPELINE/run-$RUN\"",
        ],
      },
      network = { ports = [{ name = "http", static = 8085 }] },
      checks = [{ type = "health", kind = "tcp", port = "http", every = "5s", timeout = "2s" }],
      resources = { cpu = 100, memory = 64 },
    },
  ],
}
