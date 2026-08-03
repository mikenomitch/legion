# Per-node OpenTelemetry Collector that ships every task's stdout/stderr off the
# node — a "daemonset" / system job.
#
# Placement = `count = { num = 1, per = "id" }` places exactly ONE allocation per
# node, because the scheduler groups eligible nodes by their unique node id.
# As nodes join the cluster, each gets its own collector.
#
# How it reads the logs: the collector runs as an `exec` task, so it is a child
# of the worker and inherits the worker's working directory — the same directory
# that contains `.legion/`. The relative `filelog` glob therefore matches every
# allocation's log files on that node.
#
# REQUIREMENTS / CAVEATS (read these):
#   1. The `otelcol-contrib` binary must already be installed on every node and
#      on PATH. The exec task runs a host binary; Legion does not download it.
#      The *contrib* distribution is required (the core distro lacks `filelog`).
#   2. Set `otlpEndpoint` below to a real OTLP logs backend. Do NOT use the
#      `debug`/console exporter in this job form: the collector's own stdout is
#      itself captured by Legion, so a console exporter would feed the collector
#      its own output. We keep the collector's self-logging at `warn` to keep
#      self-ingestion negligible; a real backend exporter does not echo to
#      stdout, so there is no amplification loop.
#   3. If you point workers at an absolute LEGION_WORKER_STATE_PATH, set `cwd`
#      on the task (or make the include glob absolute) so the collector can find
#      the files.
#   4. Container task runtime cannot do this: it has no host volume mounts, so a
#      containerized collector cannot read the node's log files. Use `exec`.

let otlpEndpoint = "http://localhost:4318" in  # <-- EDIT: your OTLP logs backend

let collectorConfig = m%"
  receivers:
    filelog:
      include:
        - .legion/workers/*/logs/*/stdout.log
        - .legion/workers/*/logs/*/stderr.log
      start_at: end
      poll_interval: 500ms
      include_file_path: true
      operators:
        - type: regex_parser
          parse_from: attributes["log.file.path"]
          regex: 'logs/(?P<allocation_id>\d+)/(?P<stream>stdout|stderr)\.log$'
        - type: move
          from: attributes.allocation_id
          to: resource["legion.allocation.id"]
        - type: move
          from: attributes.stream
          to: resource["legion.stream"]
  processors:
    batch: {}
    resource:
      attributes:
        - key: service.name
          value: legion-tasks
          action: upsert
  exporters:
    otlphttp:
      endpoint: ${env:OTLP_ENDPOINT}
  service:
    telemetry:
      logs:
        level: warn
    pipelines:
      logs:
        receivers: [filelog]
        processors: [resource, batch]
        exporters: [otlphttp]
"%
in

{
  name = "otel-log-shipper",
  task = {
    name = "otel-collector",
    runtime = "exec",
    lifecycle = "service",
    count = { num = 1, per = "id" },  # one allocation per node
    config = {
      command = ["otelcol-contrib", "--config", "/tmp/legion/otel-logs.yaml"],
      env = { OTLP_ENDPOINT = otlpEndpoint },
      files = [
        { path = "/tmp/legion/otel-logs.yaml", content = collectorConfig },
      ],
    },
  },
}
