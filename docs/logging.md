# Logging

Legion captures each task's stdout and stderr, makes it available through the
CLI, the MCP tools, and the web UI (with live tailing), rotates it so a noisy
task cannot fill a worker's disk, and is ready to ship to any external log
system. Legion's *own* control-plane traces can additionally be exported over
OpenTelemetry (OTLP).

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

Capture is wired through the task plugin runtime: the worker sets a log
directory on `TaskRuntime`, and the built-in `exec` and `container` task kinds
redirect their child process to those files
([`configure_task_log_capture`](../src/task_plugins/mod.rs)). Because the task
writes to a **real file descriptor** (not a pipe to the worker), logging keeps
working — and tasks keep running — across worker restarts.

> **Note on custom task plugins:** a plugin captures logs automatically as long
> as it spawns its work via `spawn_task_process` after calling
> `configure_task_log_capture(&mut cmd, runtime)` (as `exec`/`container` do). A
> plugin that manages its own process output (e.g. a runtime that writes
> elsewhere) is responsible for its own capture.

---

## The log "plugins" (sinks)

Legion does **not** run a separate per-task logging process (Nomad's `logmon`
model). Instead, log handling is expressed as an in-process, pluggable
**`LogSink`** ([`src/worker/log_sink.rs`](../src/worker/log_sink.rs)):

```rust
pub trait LogSink: Send + Sync {
    fn name(&self) -> &'static str;
    fn on_start(&self, resource: &LogResource) -> R<()> { Ok(()) }   // task started
    fn maintain(&self, resource: &LogResource) -> R<()> { Ok(()) }   // periodic upkeep
}
```

The always-on default is **`FileLogSink`**, which:

- writes the `meta.json` **resource manifest** when a task starts, and
- rotates/prunes the stream files on a periodic worker sweep.

This is the extension seam for the future (e.g. a native OTLP-logs pusher). For
today, **external shipping is done by a log collector tailing the files** — the
same division of labour Nomad, Docker (`json-file`), and Kubernetes use, and the
reason the `meta.json` manifest exists (see
[shipping](#shipping-logs-externally)).

### The resource manifest (`meta.json`)

Each allocation's log directory contains a manifest a collector can read to
attach correct OpenTelemetry **resource** attributes without re-deriving them:

```json
{
  "schema": "legion.task-logs/v1",
  "resource": {
    "service.name": "web",
    "legion.job.id": "web",
    "legion.allocation.id": 42,
    "legion.node.id": "node-1",
    "legion.task.name": "api",
    "legion.task.kind": "exec"
  },
  "streams": { "stdout": "stdout.log", "stderr": "stderr.log" },
  "rotation": { "maxFileBytes": 10485760, "maxFiles": 5, "retentionDays": 7 }
}
```

`service.name` follows OpenTelemetry semantic conventions; the `legion.*` keys
carry orchestrator identity a collector can promote to labels.

---

## Rotation & retention

Rotation works out of the box with **zero configuration**. A periodic worker
sweep checks each active stream and, when the active file exceeds the size cap,
performs a **copy-truncate** rotation: the active file is copied aside to
`stdout.log.1` (existing generations shift up to `.N`) and then truncated in
place. Because tasks write with `O_APPEND`, the task's next write lands at the
new end of the (now empty) active file — no sparse hole, no lost offset, and no
need to signal the task.

Rotated generations beyond `maxFiles`, or older than `retentionDays`, are
deleted.

### Configuration

Tuned via environment variables on the **worker** (consistent with
`LEGION_WORKER_STATE_PATH`); all are optional:

| Env var | Default | Meaning |
| --- | --- | --- |
| `LEGION_LOG_MAX_FILE_BYTES` | `10485760` (10 MiB) | Rotate a stream once its active file exceeds this. |
| `LEGION_LOG_MAX_FILES` | `5` | Rotated generations to keep per stream. |
| `LEGION_LOG_RETENTION_DAYS` | `7` | Delete rotated files older than this. |

Example — a worker that keeps less on disk:

```bash
LEGION_LOG_MAX_FILE_BYTES=2097152 \
LEGION_LOG_MAX_FILES=3 \
LEGION_LOG_RETENTION_DAYS=2 \
legion start worker
```

Defaults live in [`src/constants.rs`](../src/constants.rs)
(`DEF_TASK_LOG_*`); the engine is in
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

`--follow` polls and prints only newly-written bytes, tracked per
(allocation, stream) byte offset, so it stays efficient on chatty tasks and
recovers cleanly when a stream rotates.

### MCP

The `legion_job_logs` tool exposes the same capability to agents (read-only):

```json
{ "name": "web", "stream": "stderr", "tail": true, "max_bytes": 8192 }
```

### Web UI

Open a job and select the **Logs** tab, or a task's detail page (logs are scoped
to that task's allocations). The viewer is a live terminal pane that follows
best practices for tailing:

- **Follow** auto-scrolls to the newest line and **auto-pauses when you scroll
  up**, resuming when you return to the bottom.
- stdout and stderr are visually distinguished (and filterable).
- **Wrap**, **Clear**, **Download**, **Pause**, and a per-allocation filter.
- The buffer is bounded (5,000 lines) to keep the page responsive, and a banner
  appears when older lines have rotated out of the visible window.

Under the hood the UI polls `GetAllocationLogs` and appends only new bytes using
the per-stream `nextOffset`, the same incremental-tail mechanism as the CLI.

### How it is wired

1. Workers capture stdout/stderr to per-allocation files (above).
2. `Worker.GetTaskLogs` reads those files on the node (byte cap, tail, offset).
3. `Manager.GetAllocationLogs` resolves a job (or allocation id) to its
   allocations, locates each host node, and proxies the read.
4. `legion job logs`, the `legion_job_logs` MCP tool, and the UI surface it.

---

## Shipping logs externally

Legion writes plain, stably-named files plus a resource manifest, so any file
-tailing collector can ship them with **no Legion-side configuration**. Point a
collector at the worker log root:

```
<worker-state-dir>/logs/<allocation-id>/{stdout,stderr}.log
# default: .legion/workers/*/logs/*/*.log
```

Ready-to-use examples live in [`configs/observability/`](../configs/observability/):

### OpenTelemetry Collector (recommended)

[`configs/observability/otel-collector.yaml`](../configs/observability/otel-collector.yaml)
uses the `filelog` receiver to tail the stream files, parses the allocation id
from the path, and (optionally) enriches from `meta.json`. Run it next to each
worker:

```bash
otelcol --config configs/observability/otel-collector.yaml
```

It exports OTLP logs to whatever backend you configure (Loki, Elasticsearch,
an OTLP endpoint, …). This is the recommended baseline: the files are already
structured for it and the manifest supplies OpenTelemetry resource attributes.

### Vector

[`configs/observability/vector.toml`](../configs/observability/vector.toml) is an
equivalent recipe using [Vector](https://vector.dev):

```bash
vector --config configs/observability/vector.toml
```

### Why a collector instead of built-in forwarding?

Shipping is intentionally **not** in Legion's hot path: a task writes to a file
descriptor that survives worker restarts, and a collector tails the files. This
keeps the worker simple and lets operators use the mature, backpressure-aware
shippers they already run. The `LogSink` trait remains the seam if a native
in-process exporter is ever warranted.

---

## OpenTelemetry traces for Legion itself

Separately from task logs, Legion's **own** control-plane spans (the `tracing`
instrumentation across the manager and worker) can be exported over **OTLP/HTTP**
to any OpenTelemetry backend (Tempo, Jaeger, Honeycomb, an OTel Collector, …).

It is opt-in and configured with the standard OpenTelemetry environment
variables — the baseline is "set one env var":

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
legion start manager      # or: legion start worker
# → legion: exporting OTLP traces to http://localhost:4318/v1/traces
```

Recognized configuration (all optional):

| Env var | Default | Meaning |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Base collector URL; `/v1/traces` is appended. Setting it enables export. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | — | Exact traces endpoint (used verbatim). |
| `OTEL_SERVICE_NAME` | `legion` | `service.name` resource attribute. |
| `LEGION_OTLP` | `0` | Force-enable at the default endpoint without setting an endpoint var. |

Notes:

- Export is limited to long-running servers (`start manager` / `start worker`);
  short CLI invocations never spin up an exporter.
- The exporter is OTLP/**HTTP** (protobuf, port 4318 by convention) so it adds no
  second gRPC stack to the build.
- Spans carry `service.name` and `service.version` resource attributes.
- The pre-existing local trace files (`legion start … --o11y` → `./traces`) are
  unchanged; OTLP export is additive.

Implementation: [`src/otel.rs`](../src/otel.rs), wired in
[`src/tracing_util.rs`](../src/tracing_util.rs).
