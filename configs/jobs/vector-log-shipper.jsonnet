// Per-node Vector (https://vector.dev) that ships every task's stdout/stderr off
// the node — the Vector equivalent of otel-log-shipper.jsonnet.
//
// Placement: `count: { num: 1, per: 'id' }` => exactly one allocation per node
// (the scheduler groups nodes by unique node id).
//
// Reads logs as an `exec` task that inherits the worker's working directory
// (which contains `.legion/`), so the relative `include` glob matches every
// allocation's files on that node.
//
// REQUIREMENTS / CAVEATS:
//   1. The `vector` binary must already be installed on every node and on PATH.
//      The exec task runs a host binary; Legion does not download it.
//   2. Set `lokiEndpoint` below (or swap the sink) to a real destination. Do NOT
//      use the `console` sink in this job form — Vector's own stdout is captured
//      by Legion, which would feed Vector its own output. We set VECTOR_LOG=warn
//      to keep self-ingestion negligible; a real sink does not echo to stdout.
//   3. Container task runtime cannot do this (no host volume mounts). Use `exec`.

local lokiEndpoint = 'http://localhost:3100';  // <-- EDIT: your Loki endpoint

local vectorConfig = |||
  data_dir = "/tmp/legion/vector"

  [sources.legion]
  type = "file"
  include = [".legion/workers/*/logs/*/stdout.log", ".legion/workers/*/logs/*/stderr.log"]
  read_from = "end"

  [transforms.enrich]
  type = "remap"
  inputs = ["legion"]
  source = '''
  parsed = parse_regex(.file, r'logs/(?P<allocation_id>\d+)/(?P<stream>stdout|stderr)\.log$') ?? {}
  .allocation_id = parsed.allocation_id
  .stream = parsed.stream
  .service = "legion-tasks"
  '''

  [sinks.out]
  type = "loki"
  inputs = ["enrich"]
  endpoint = "${LOKI_ENDPOINT}"
  encoding.codec = "json"
  labels.service = "legion-tasks"
  labels.allocation_id = "{{ allocation_id }}"
|||;

{
  name: 'vector-log-shipper',
  task: {
    name: 'vector',
    runtime: 'exec',
    lifecycle: 'service',
    count: { num: 1, per: 'id' },  // one allocation per node
    config: {
      command: ['vector', '--config', '/tmp/legion/vector.toml'],
      env: { LOKI_ENDPOINT: lokiEndpoint, VECTOR_LOG: 'warn' },
      files: [
        { path: '/tmp/legion/vector.toml', content: vectorConfig },
      ],
    },
  },
}
