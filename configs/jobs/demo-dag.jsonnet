// A multi-stage data pipeline DAG for showcasing the UI visualization.
//
//   bootstrap ──► extract ──► validate ─┐
//             └─► fetch   ──► transform ─┤──► aggregate ──► publish
//                       └► audit (sidecar)             └──► notify
//
// Long-running "service" tasks stay healthy so the graph shows live status;
// "batch" stages complete quickly to unblock their successors.
local service(name, group, deps=[]) = {
  name: name,
  kind: 'exec',
  lifecycle: 'service',
  group: group,
  dependsOn: deps,
  config: { command: ['sleep', '3600'] },
  resources: { cpus: 0.1, memory: 128 },
};

local batch(name, group, seconds, deps=[]) = {
  name: name,
  kind: 'exec',
  lifecycle: 'batch',
  group: group,
  dependsOn: deps,
  config: { command: ['sleep', std.toString(seconds)] },
  resources: { cpus: 0.1, memory: 128 },
};

local sidecar(name, group, deps=[]) = {
  name: name,
  kind: 'exec',
  lifecycle: 'sidecar',
  group: group,
  dependsOn: deps,
  config: { command: ['sleep', '3600'] },
  resources: { cpus: 0.1, memory: 64 },
};

local dep(task, condition) = { task: task, condition: condition };

{
  name: 'demo-dag',
  tasks: [
    // Stage 1 — entrypoint
    batch('bootstrap', 'bootstrap', 2),

    // Stage 2 — fan out from bootstrap
    batch('extract', 'ingest', 3, [dep('bootstrap', 'success')]),
    batch('fetch', 'ingest', 3, [dep('bootstrap', 'success')]),
    sidecar('audit', 'ingest', [dep('extract', 'start')]),

    // Stage 3 — long-running processors
    service('validate', 'process', [dep('extract', 'success')]),
    service('transform', 'process', [dep('fetch', 'success')]),

    // Stage 4 — fan in (join two branches)
    service('aggregate', 'process', [dep('validate', 'start'), dep('transform', 'start')]),

    // Stage 5 — outputs
    service('publish', 'output', [dep('aggregate', 'start')]),
    batch('notify', 'output', 8, [dep('aggregate', 'start')]),
  ],
}
