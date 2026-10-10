# Logging

Legion captures each task's stdout and stderr. You can read the output through
the CLI, the MCP tools, and the web UI (with live tailing). Legion rotates the
files so a noisy task can't fill a worker's disk, and any standard log
collector can ship them to an external system. Legion can also export its own
control-plane traces over OpenTelemetry (OTLP).

This document covers:

- [How task log capture works](#how-task-log-capture-works)
- [The log "plugins" (sinks)](#the-log-plugins-sinks)
- [Rotation & retention (and how to configure it)](#rotation--retention)
- [Reading & tailing logs (CLI, MCP, UI)](#reading--tailing-logs)
- [Shipping logs to an external system](#shipping-logs-externally)
- [OpenTelemetry traces for Legion itself](#opentelemetry-traces-for-legion-itself)

---

## How task log capture works

When a worker starts a task, it points the task process's stdout and stderr at
two append-mode files in a per-allocation directory:

```
<worker-state-dir>/logs/<allocation-id>/
├── stdout.log         # active stream files
├── stderr.log
├── stdout.log.1       # rotated generations (newest = .1)
├── stderr.log.1
├── …
└── meta.json          # resource manifest for log shippers
```

The state dir defaults to `.legion/workers/<worker-id>/` (override the worker
state path with `LEGION_WORKER_STATE_PATH`).

The per-allocation log directory is owner-only (`0700` on Unix). Captured
output can contain secrets, so other tasks and local users on the node can't
read it. Tasks never need to enter the directory themselves, because the
worker opens the capture file descriptors before spawning the task.

Capture runs through the task plugin runtime. The worker sets a log directory
on `TaskInstance`, and the built-in `exec` and `container` task runtimes
redirect their child process to those files
([`configure_task_log_capture`](../src/task_plugins/mod.rs)). The task writes
to a file descriptor instead of a pipe to the worker. So logging keeps working,
and tasks keep running, across worker restarts.

> **Note on custom task plugins:** a plugin captures logs automatically if it
> spawns its work with `spawn_task_process` after calling
> `configure_task_log_capture(&mut cmd, runtime)` (as `exec`/`container` do). A
> plugin that manages its own process output (for example, a runtime that
> writes elsewhere) has to handle its own capture.

---

## The log "plugins" (sinks)

Legion doesn't run a separate logging process per task (Nomad's `logmon`
model). Log handling is an in-process, pluggable **`LogSink`**
([`src/worker/log_sink.rs`](../src/worker/log_sink.rs)):

```rust
pub trait LogSink: Send + Sync {
    fn name(&self) -> &'static str;
    fn on_start(&self, resource: &LogResource) -> R<()> { Ok(()) }   // task started
    fn maintain(&self, resource: &LogResource) -> R<()> { Ok(()) }   // periodic upkeep
}
```

The default sink is always on. It's called **`FileLogSink`**, and it:

- Writes the `meta.json` resource manifest when a task starts.
- Rotates and prunes the stream files on a periodic worker sweep.

The trait leaves room for future sinks, such as a native OTLP-logs pusher.
Today, you ship logs externally by running a log collector that tails the
files. Nomad, Docker (`json-file`), and Kubernetes split the work the same way.
The `meta.json` manifest exists to support this (see
[shipping](#shipping-logs-externally)).

### The resource manifest (`meta.json`)

Each allocation's log directory contains a manifest. A collector can read it
to attach the right OpenTelemetry resource attributes without working them out
itself:

```json
{
  "schema": "legion.task-logs/v1",
  "resource": {
    "service.name": "web",
    "legion.job.id": "web",
    "legion.allocation.id": 42,
    "legion.node.id": "node-1",
    "legion.task.name": "api",
    "legion.task.runtime": "exec"
  },
  "streams": { "stdout": "stdout.log", "stderr": "stderr.log" },
  "rotation": { "maxFileBytes": 10485760, "maxFiles": 5, "retentionDays": 7 }
}
```

`service.name` follows OpenTelemetry semantic conventions. The `legion.*` keys
identify the job, allocation, node, and task, and a collector can promote them
to labels.

---

## Rotation & retention

Rotation works out of the box with no configuration. A periodic worker sweep
checks each active stream. When the active file exceeds the size cap, the
sweep does a copy-truncate rotation. It copies the active file to
`stdout.log.1` (existing generations shift up to `.N`), then truncates the
active file in place. Tasks write with `O_APPEND`, so the task's next write
lands at the start of the now-empty active file. There's no sparse hole, no
lost offset, and no need to signal the task.

Rotated generations beyond `maxFiles`, or older than `retentionDays`, are
deleted.

### Configuration

Tune rotation with environment variables on the worker (like
`LEGION_WORKER_STATE_PATH`). All are optional:

| Env var | Default | Meaning |
| --- | --- | --- |
| `LEGION_LOG_MAX_FILE_BYTES` | `10485760` (10 MiB) | Rotate a stream once its active file exceeds this. |
| `LEGION_LOG_MAX_FILES` | `5` | Rotated generations to keep per stream. |
| `LEGION_LOG_RETENTION_DAYS` | `7` | Delete rotated files older than this. |

For example, a worker that keeps less on disk:

```bash
LEGION_LOG_MAX_FILE_BYTES=2097152 \
LEGION_LOG_MAX_FILES=3 \
LEGION_LOG_RETENTION_DAYS=2 \
legion start worker
```

Defaults live in [`src/constants.rs`](../src/constants.rs)
(`DEF_TASK_LOG_*`). The rotation code is in
[`src/worker/logs.rs`](../src/worker/logs.rs).

---

## Reading & tailing logs

### CLI

```bash
# All allocations of a job, both streams (one-shot):
legion job logs web

# A single allocation, stderr only, last 4 KiB:
legion job logs web --allocation 42 --stream stderr --max-bytes 4096 --tail

# Live tail (Ctrl-C to stop) — like `tail -f`, across the job's allocations:
legion job logs web --follow

# Machine-readable (one envelope; not combinable with --follow):
legion job logs web --json
```

Flags: `--allocation <ID>`, `--stream stdout|stderr|all` (default `all`),
`--max-bytes <N>` (`0` = the worker's 256 KiB default window), `--tail` (trailing
bytes), `--follow`/`-f` (stream until interrupted).

`--follow` polls and prints only newly written bytes. It tracks a byte offset
per (allocation, stream), so it stays efficient on chatty tasks and recovers
cleanly when a stream rotates.

### MCP

The `legion_job_logs` tool exposes the same capability to agents (read-only):

```json
{ "name": "web", "stream": "stderr", "tail": true, "max_bytes": 8192 }
```

### Web UI

Open a job and select the **Logs** tab, or open a task's detail page (logs
there are scoped to that task's allocations). The viewer is a live terminal
pane:

- **Follow** auto-scrolls to the newest line. It pauses when you scroll up and
  resumes when you return to the bottom.
- stdout and stderr are styled differently, and you can filter by stream.
- **Wrap**, **Clear**, **Download**, **Pause**, and a per-allocation filter.
- The buffer holds up to 5,000 lines to keep the page responsive. A banner
  appears when older lines have scrolled out of the buffer.

The UI polls `GetAllocationLogs` and appends only new bytes using the
per-stream `nextOffset`. This is the same incremental tail the CLI uses.

### How it is wired

1. Workers capture stdout/stderr to per-allocation files (above).
2. `Worker.GetTaskLogs` reads those files on the node (byte cap, tail, offset).
3. `Manager.GetAllocationLogs` resolves a job (or allocation id) to its
   allocations, locates each host node, and proxies the read.
4. `legion job logs`, the `legion_job_logs` MCP tool, and the UI surface it.

---

## Shipping logs externally

There is nothing to enable in Legion. Workers already write plain files with
stable names. To ship logs, run a standard file-tailing agent (the
OpenTelemetry Collector or Vector) next to each worker and point it at those
files. Here is the whole process.

### Step 1 — Find the log files on the worker host

Each worker writes (relative to the **worker process's working directory**):

```
.legion/workers/<worker-id>/logs/<allocation-id>/stdout.log
.legion/workers/<worker-id>/logs/<allocation-id>/stderr.log
.legion/workers/<worker-id>/logs/<allocation-id>/meta.json   # resource attributes
```

> **Gotcha:** that path is relative to wherever you launched `legion start
> worker`. If you run the worker from `/opt/legion`, the glob is
> `/opt/legion/.legion/workers/*/logs/*/*.log`. Confirm the absolute location
> with:
>
> ```bash
> find / -path '*/.legion/workers/*/logs/*/stdout.log' 2>/dev/null | head
> ```
>
> (`LEGION_WORKER_STATE_PATH` overrides the base directory.)

Run the collector on the same host as the worker so it can read those local
files. You need one collector per worker host.

### Step 2 — Install a collector that can tail files

Use the OpenTelemetry Collector **contrib** distribution. The `filelog`
receiver is not in the core `otelcol` build:

```bash
# Binary: download "otelcol-contrib" from
#   https://github.com/open-telemetry/opentelemetry-collector-releases/releases
# Docker image:
docker pull otel/opentelemetry-collector-contrib:latest
```

(Or use [Vector](https://vector.dev), which has file tailing built in.)

### Step 3 — Run it with the provided config and confirm logs flow

The repo ships a working config at
[`configs/observability/otel-collector.yaml`](../configs/observability/otel-collector.yaml).
It tails the files, parses the allocation id and stream from the path, and
prints to the collector's own stdout with the `debug` exporter. This lets you
check the pipeline before you connect a backend:

```bash
# from the worker's working directory (so the relative glob matches):
otelcol-contrib --config /path/to/configs/observability/otel-collector.yaml
```

Run a job that prints something, then watch the collector's console. You
should see your task's lines, tagged with `legion.allocation.id` and
`legion.stream`. If you see them, shipping works. The rest is choosing a
destination.

### Step 4 — Point it at your real backend

Edit the `exporters:` block in the config and swap `debug` for your backend.
For example:

```yaml
exporters:
  # A) Any OTLP logs endpoint (Grafana Cloud, Honeycomb, another collector, …)
  otlphttp/backend:
    endpoint: https://otlp.example.com           # /v1/logs is appended
    headers: { "authorization": "Bearer ${TOKEN}" }

  # B) Grafana Loki (Loki 3.x exposes a native OTLP endpoint)
  otlphttp/loki:
    endpoint: http://loki:3100/otlp

  # C) Elasticsearch (contrib distro)
  elasticsearch:
    endpoints: ["http://elasticsearch:9200"]
```

then reference it in the pipeline:

```yaml
service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [resource, batch]
      exporters: [otlphttp/loki]   # whichever you configured
```

There is no Legion-side step. Vector works the same way
([`configs/observability/vector.toml`](../configs/observability/vector.toml)):
swap its `[sinks.out]` for `loki`, `elasticsearch`, `http`, and so on.

### Try the whole thing locally (Docker Compose)

[`configs/observability/docker-compose.yaml`](../configs/observability/docker-compose.yaml)
brings up a collector (+ optional Loki/Grafana) wired to tail a log directory you
mount in. From the repo root, after a worker has produced some logs:

```bash
# Point it at your worker's log root and start the collector:
LEGION_LOG_DIR="$(pwd)/.legion" docker compose \
  -f configs/observability/docker-compose.yaml up
```

The collector container prints the parsed log records (`docker compose logs
otel-collector`). Uncomment the `loki` and `grafana` services in that file to
view them in Grafana at <http://localhost:3000> (Explore → Loki).

### A note on rotation + at-least-once delivery

Legion rotates with copy-truncate (see [above](#rotation--retention)). As with
`logrotate`'s `copytruncate` and any file tailer, the collector can miss a few
bytes written between its last read and the truncate. For most workloads this
doesn't matter. If you need stricter delivery, raise
`LEGION_LOG_MAX_FILE_BYTES` (to rotate less often), lower the collector's
`poll_interval`, or ship from the application itself.

### Run the shipper as a per-node Legion job (daemonset)

You don't have to manage the collector separately. You can run it as a Legion
job, one per node, so the cluster schedules and supervises it for you. Two
ready-made jobspecs ship in [`configs/jobs/`](../configs/jobs/):

```bash
legion job run configs/jobs/otel-log-shipper.ncl     # OpenTelemetry Collector
# or
legion job run configs/jobs/vector-log-shipper.ncl   # Vector
```

How they work:

- **One per node** via `count = { num = 1, per = "id" }`. Legion's scheduler
  groups eligible nodes by their unique node id, so `per = "id"` places one
  allocation on every node (the daemonset or system-job pattern). New nodes
  get a shipper automatically.
- **Reads every allocation's logs** because the shipper runs as an `exec`
  task. It is a child of the worker and inherits the worker's working
  directory (the one containing `.legion/`). So the relative `filelog`/`file`
  glob `.legion/workers/*/logs/*/*.log` matches all of that node's task logs.
- The exec task's `files` field writes the collector or Vector config onto the
  node, so you don't need a separate way to distribute config.

Edit the endpoint at the top of the jobspec (`otlpEndpoint` / `lokiEndpoint`)
before running.

**Requirements and limits (important):**

- The `otelcol-contrib` (or `vector`) **binary must already be installed on
  every node** and on `PATH`. The `exec` task runs a host binary, and Legion
  doesn't download it. (You need the OTel Collector contrib distro, because
  core lacks `filelog`.)
- **Don't use the `debug`/`console` exporter in the job form.** Legion
  captures the shipper's own stdout, so a console exporter would feed the
  shipper its own output. The jobspecs use a backend exporter and set the
  shipper's own logging to `warn`, so very little of its output gets shipped.
  A backend exporter doesn't echo to stdout, so there's no feedback loop.
- If workers use an absolute `LEGION_WORKER_STATE_PATH`, set the task's `cwd`
  (or make the glob absolute) so the relative path still resolves.
- **The shipped jobspecs use `exec`.** A `container` task can't see the
  worker's working directory, so a containerized collector needs the log
  directory mounted in. Declare it as a read-only `hostVolumes` entry in the
  worker config and claim it with a `host` volume (see
  [docs/volumes.md](volumes.md)). The container's user must also be able to
  read the owner-only (`0700`) per-allocation log directories.

### Why a collector instead of built-in forwarding?

Shipping stays out of Legion's hot path on purpose. A task writes to a file
descriptor that survives worker restarts, and a collector tails the files.
This keeps the worker simple and lets operators use the mature,
backpressure-aware shippers they already run. If a native in-process exporter
is ever needed, it can be added as a `LogSink`.

---

## OpenTelemetry traces for Legion itself

Separate from task logs, Legion can export its own control-plane spans (the
`tracing` instrumentation across the manager and worker) over OTLP/HTTP. Any
OpenTelemetry backend works (Tempo, Jaeger, Honeycomb, an OTel Collector, and
so on).

Export is opt-in, and you configure it with the standard OpenTelemetry
environment variables. In the simplest case, you set one variable:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
legion start manager      # or: legion start worker
# → legion: exporting OTLP traces to http://localhost:4318/v1/traces
```

Recognized configuration (all optional):

| Env var | Default | Meaning |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (none) | Base collector URL. `/v1/traces` is appended. Setting it enables export. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | (none) | Exact traces endpoint (used as is). |
| `OTEL_SERVICE_NAME` | `legion` | `service.name` resource attribute. |
| `LEGION_OTLP` | `0` | Force-enable at the default endpoint without setting an endpoint var. |

Notes:

- Only long-running servers (`start manager` / `start worker`) export. Short
  CLI invocations never start an exporter.
- The exporter uses OTLP over HTTP (protobuf, port 4318 by convention), so it
  doesn't add a second gRPC stack to the build.
- Spans carry `service.name` and `service.version` resource attributes.
- Local trace files (`legion start … --o11y` → `./traces`) still work as
  before. OTLP export runs alongside them.

Implementation: [`src/otel.rs`](../src/otel.rs), wired in
[`src/tracing_util.rs`](../src/tracing_util.rs).
