# Plan: A Device Model, Model-Scale Artifacts, and Safer Plugin Seams

> **Judgement calls:** [`calls/device-model-and-plugin-seams-plan_calls.md`](../calls/device-model-and-plugin-seams-plan_calls.md)

Status: **IN EXECUTION.** Covers review findings PLG1–PLG5 and AI6 from
[architecture-review-2026-11-07.md](../architecture-review-2026-11-07.md).
The status of each phase is kept in [§8](#8-implementation-phases); what was
deliberately deferred is listed in [§12](#12-deferred).

## 1. Problem statement

The user's words: "There is no device model, so GPUs effectively don't work.
... The artifact cache is too small for model weights."

Concretely, today:

- **Devices (PLG2).** A worker type can declare `resources = { gpu = ["g0",
  "g1"] }`, and a task can ask for `gpu = ["g0"]` by *naming the device*. A
  task cannot ask for "two GPUs" against such a node (an amount never fits an
  item set), nothing tells the task which devices it got (no
  `CUDA_VISIBLE_DEVICES`, no CDI, no `nvidia` anywhere in `src/`), the
  container runtime denies every device (`linux.resources.devices = [{ allow =
  false }]`, `crates/worker/src/task_plugins/oci_bundle.rs:1688`), the device list is static
  config with no health, and quota caps cpu, memory, and cores only. A GPU job
  today means `exec` plus a hand-written `CUDA_VISIBLE_DEVICES` and a promise
  that nobody else uses the same card.
- **Model weights (AI6).** The artifacts class of the node content store
  defaults to 10% of the disk clamped to 1–20 GiB
  (`crates/common/src/content_store.rs:88`), smaller than one 70B checkpoint, and ORAS
  layers (the shape model weights ship in) are never cached.
- **Plugins (PLG1, PLG5).** Five dylib load sites call `Box<dyn Trait>`
  methods with no `catch_unwind`; a scheduling or rollout plugin that panics
  takes the manager's eval thread with it. Node checks are dlopened on every
  pass and return an untyped map.
- **Runtimes (PLG3).** The OCI runtime is one node-wide binary that must be
  runc or crun, so gVisor (`runsc`) and Kata, the usual sandboxes for
  untrusted agent code, are refused.
- **Admission (PLG4).** Operators cannot say "no `:latest` images" or "every
  service names an owner": app object kinds have CEL `rules`, jobs do not.

## 2. Prior art

- **Kubernetes device plugins** (gRPC over a unix socket): `ListAndWatch`
  streams devices with health, `Allocate` returns env, mounts, device nodes,
  and (since 1.28) CDI device names. Device IDs are opaque; topology comes
  through the Topology Manager. **DRA** adds claims, structured parameters,
  and sharing. We take the shape of `Allocate` (CDI container edits) and the
  idea that the plugin owns vendor knowledge. We reject gRPC for now (§calls)
  and DRA claims (deferred).
- **CDI** (Container Device Interface, `cncf-tags/container-device-interface`):
  a JSON/YAML spec per vendor kind (`nvidia.com/gpu`) whose devices carry
  `containerEdits` — `env`, `deviceNodes`, `mounts`, `hooks`. We take the
  `containerEdits` shape verbatim, so a plugin can return CDI device names
  and Legion resolves them from `/etc/cdi` and `/var/run/cdi`.
- **Nomad device plugins** (`nvidia`): fingerprint groups named
  `<vendor>/<type>/<name>` with attributes; a jobspec `device "nvidia/gpu" {
  count = 2, constraint {...}, affinity {...} }`; reservation returns env and
  mounts. We take the naming (`gpu`, `nvidia/gpu`, `nvidia/gpu/<model>`), the
  count + constraint + affinity request, and the "one request is served from
  one device group" rule.
- **Slurm GRES** with `gres.conf` topology (cores/NUMA per GPU) and NVML
  autodetect. We take topology-aware packing (prefer devices that share an
  NVLink domain or NUMA node).
- **Kubernetes `RuntimeClass`**: a named handler per class, with the node
  advertising which classes it has. We take that for PLG3.
- **Kubernetes `ValidatingAdmissionPolicy`**: CEL over the object, failure
  messages per rule. We take it for PLG4, reusing Legion's CEL bounds.
- **Dragonfly, Spegel, SOCI/Nydus** for P2P and lazy loading: evaluated in
  §12, deferred.

## 3. Jobs to be done, by persona

**Job author / ML engineer**
- When I train or serve a model, I want to ask for "two GPUs with at least 40
  GiB each" and have my process see exactly those two, so I can stop
  hard-coding device numbers and colliding with other jobs.
- When my container needs a GPU, I want the device files and driver
  libraries to just be there, so I do not have to run privileged.
- When I ship a 140 GB checkpoint as an OCI artifact, I want the second
  start on a node to skip the download, so a restart takes seconds not
  twenty minutes.

**Cluster operator**
- When I add a GPU node, I want Legion to discover its GPUs (model, memory,
  topology) without me typing IDs into config, so the inventory is always
  true.
- When a GPU falls off the bus or reports XID errors, I want new work kept
  off that card, so one bad device does not fail job after job.
- When I install a plugin that turns out to be buggy, I want the manager to
  keep running and tell me the plugin was disabled, so a plugin bug is not an
  outage.
- When I offer gVisor for untrusted code, I want jobs to pick it by name and
  land only where it is installed.

**Security / platform admin**
- When teams share a GPU pool, I want to cap how many GPUs each namespace
  holds, so one team cannot take the whole pool.
- When a team submits a job, I want my house rules (no `latest` tags, an
  owner on every service) enforced at submit with a message that names the
  rule, so I do not review every spec by hand.

**On-call responder**
- When a GPU job is pending, I want `legion node info` to show each node's
  devices and which are unhealthy or taken, so I can see why it does not
  place.

**Autonomous agent**
- When I place GPU work, I want the device request, the assignment, and the
  refusal reasons as structured JSON (`job plan -j`, `alloc` JSON, `node
  info -j`), so I can retry with a smaller ask without parsing prose.

*Not in scope:* fractional GPU sharing for job authors (MIG/MPS), and microVM
GPU passthrough (VFIO) — §12.

### User scenarios

1. **Two GPUs for a training job.** The operator adds `devices.plugins.nvidia`
   to the `gpu` worker type. The job says `resources.devices = [{ name =
   "nvidia/gpu", count = 2, constraints = [{ key = "memoryMiB", operator =
   ">=", value = 40000 }] }]`. It places on a node with two healthy free
   A100s, preferring two on the same NVLink domain; the task sees
   `CUDA_VISIBLE_DEVICES=GPU-…,GPU-…` and `LEGION_DEVICE_NVIDIA_GPU_IDS`.
2. **A card goes bad.** nvidia-smi stops reporting it; within one node-check
   pass (30 s) the instance is unhealthy, `legion node info` shows it, and
   the next job does not get it. The running job is not killed (Legion does
   not know whether the job still uses it).
3. **A namespace over its GPU quota** gets `namespace policy 'ml-teams' caps
   band 'normal' at 8 nvidia/gpu devices across ml-*; in use 8, this job adds
   2` at `job run`.

## 4. Goals and non-goals

Goals:
- A device-plugin seam (exec + JSON, `legion-device/1`) with fingerprint
  (groups, instances, attributes, health, topology) and allocate (CDI
  container edits), periodic re-checks, and static devices in config.
- Jobspec device requests with count, constraints, and affinities; the
  assignment recorded on the allocation; topology-aware packing.
- Containers get device nodes, cgroup device rules, mounts, env, and hooks;
  `exec` gets env.
- Namespace quota per device name.
- Visibility in `node info`, allocation JSON, and agent surfaces.
- Reference `nvidia` plugin and a `fake` plugin for tests.
- AI6: a model-scale default cache size with an operator ceiling, and ORAS
  layers cached by digest.
- PLG1: every dylib call contained, panicking plugins quarantined and
  visible.
- PLG3: OCI runtime classes.
- PLG4: CEL job rules on namespace policies.
- PLG5: node checks loaded once and returning a typed report.

Non-goals (this plan): MIG/MPS and fractional sharing, VFIO for the
microVM, DRA-style claims, a gRPC device-plugin API, P2P artifact
distribution, lazy-loading filesystems, job mutation policies, a secrets
provider, moving in-manager plugins to WASM, an out-of-process task runtime
API, Windows. Each has an entry in §12.

## 5. Background — what exists today

- Custom resources: `ResourcePool::from_node` reads
  `config_attrs.resources` (`crates/common/src/resources.rs:982`); `ResourceValue::Items`
  is a set of item tokens; `can_fit` needs `request ⊆ free` for items.
- The single allocation-insert choke point is
  `Store::insert_allocation_with_data` (`crates/manager/src/storage/store.rs`), called by
  the plan-apply walk and every other placement path.
- The plan-apply walk rebuilds each candidate's free pool in-transaction and
  re-checks `can_fit` (`crates/manager/src/storage/store.rs`, `apply_plan_submission_on`).
- The worker sends `HealthCheck` every 30 s (`start_node_checks`,
  `crates/worker/src/worker/daemon.rs`); the store compares before writing
  (`update_node_health_checks`), the write is status-lane and forwardable to
  the lane leader, and `cache_node` re-projects the node's free pool.
- Worker-type top-level blocks that need no proto field ride the
  `artifacts` payload (`WorkerArtifactsPayload`, `crates/common/src/structs/config.rs`).
- `TaskAllocation.task_resources` is the task's declared resources
  (`crates/manager/src/manager/dispatcher.rs`).
- Exec+JSON plugin conventions: `crate::util::run_json_command` (one JSON
  request on stdin, one reply on stdout, timeout), used by the cluster CA and
  keyring `exec` providers; the volume exec ABI adds a manifest and a
  conformance harness.
- CEL: `crates/common/src/structs/object_rules.rs` compiles and bounds rules for object
  kinds.
- Content store budgets: `BudgetSpec::Share { percent, min, max }`
  (`crates/common/src/content_store.rs`).
- Dylib loads: `crates/worker/src/worker/node_check.rs`, `crates/worker/src/rollout_strategy.rs`,
  `crates/manager/src/manager/scheduler/policy.rs`, `crates/worker/src/task_plugins/mod.rs`,
  `crates/worker/src/artifacts/fetcher.rs`, all through
  `plugin_runtime::open_plugin_library`.

## 6. Proposed design

### 6.1 Device groups and instances

A node advertises **device groups**. A group is identical devices from one
plugin, named `<vendor>/<type>/<model>`:

```json
{
  "vendor": "nvidia", "type": "gpu", "model": "A100-SXM4-80GB",
  "plugin": "nvidia",
  "attributes": { "memoryMiB": 81920, "driver": "550.54.15", "computeCapability": "8.0" },
  "instances": [
    { "id": "GPU-6f2c…", "healthy": true,
      "attributes": { "index": 0, "pciBusId": "00000000:07:00.0", "numaNode": 0, "peerGroup": "nvlink-0" } }
  ]
}
```

`healthy = false` instances (with a `healthDetail`) are advertised but never
assigned.

### 6.2 The plugin protocol, `legion-device/1`

Worker type:

```nickel
devices = {
  plugins = {
    nvidia = { command = ["/opt/legion/plugins/devices/nvidia/legion-device-nvidia"],
               config = {}, timeout = "10s" },
  },
  static = [ { vendor = "xilinx", type = "fpga", model = "u250",
               instances = [ { id = "0", cdi = "xilinx.com/fpga=0" } ] } ],
  cdiSpecDirs = ["/etc/cdi", "/var/run/cdi"],   # the default
},
```

One JSON request on stdin, one JSON reply on stdout, non-zero exit or a
timeout is an error (the cluster-CA `exec` convention):

```json
{"protocol":"legion-device/1","op":"fingerprint","plugin":"nvidia","config":{}}
→ {"devices":[ <group>, ... ]}

{"protocol":"legion-device/1","op":"allocate","plugin":"nvidia","config":{},
 "device":"nvidia/gpu/A100-SXM4-80GB","ids":["GPU-6f2c…","GPU-91ab…"]}
→ {"containerEdits":{"env":["CUDA_VISIBLE_DEVICES=…"],"deviceNodes":[…],"mounts":[…],"hooks":[…]},
   "cdiDevices":["nvidia.com/gpu=GPU-6f2c…"]}
```

Fingerprint runs at registration and on every node-check pass; a plugin that
fails keeps nothing (its groups disappear) and its error is reported. An
allocate failure fails the allocation (the manager reschedules).

`legion device plugin test <command…>` runs the conformance harness against a
plugin.

### 6.3 Requests and assignment

```nickel
resources = {
  memory = 32768,
  devices = [
    { name = "nvidia/gpu", count = 2,
      constraints = [{ key = "memoryMiB", operator = ">=", value = 40000 }],
      affinities = [{ key = "peerGroup", operator = "is_set", multiplier = 1 }] },
  ],
},
```

- `name` is `type`, `vendor/type`, or `vendor/type/model`.
- `constraints`/`affinities` take the task constraint syntax, evaluated per
  instance over the group's attributes, the instance's attributes (which win),
  and `vendor`, `type`, `model`, `id`.
- One request is served from one group (same model).
- **Feasibility** (planner and the apply re-check): a node fits when some
  matching group has `count` free healthy instances that satisfy the
  constraints.
- **Assignment** happens in `insert_allocation_with_data`, inside the apply
  transaction, against committed rows: the allocation's stored `resources`
  carries `devices[i].assigned = { device, plugin, ids }`. Within a group
  the chosen ids stay inside one `peerGroup`, else one `numaNode`, when they
  fit; among the domains that fit, higher affinity wins, then the smallest
  (best fit, to limit fragmentation), then instance order. (Built that way;
  the first draft put locality above affinity, see calls 10.)
- Device affinities choose devices on the node, not the node: node scoring
  is unchanged.

### 6.4 Allocate at task start

The worker reads the assignment from `TaskAllocation.task_resources` (the
dispatcher sends the allocation's assigned `devices`), calls each plugin's
`allocate`, resolves `cdiDevices` from the CDI spec dirs (JSON specs), and
sets:

- env, always: `LEGION_DEVICE_<NAME>_IDS=<id>,<id>` (NAME is the request name
  upper-cased, non-alphanumerics as `_`), plus the plugin's env;
- `exec`: env only;
- `container`: env, `linux.devices` plus a `linux.resources.devices` allow
  rule per device node (the deny-all rule stays first), bind mounts, and OCI
  hooks;
- `sandbox`: refused at submit (no VFIO).

### 6.5 Quota

```nickel
quota = { normal = { cpu = 64000, devices = { "nvidia/gpu" = 8 } } }
```

A request is charged against a key when the two names could describe the
same device (each component they both name is equal), so `gpu` counts against
`nvidia/gpu`. Declared (count × devices) and live (assigned) are charged like
cpu and memory.

### 6.6 Visibility

- `legion node info` prints a Devices section (group, instance, health,
  attributes, failing plugins). Which allocation holds an instance is not
  shown there (the node record has no allocations); `legion ledger` shows
  each allocation's resources, assignment included.
- The allocation JSON carries the assignment in `resources.devices`.
- The MCP tool `legion_node_devices` returns the device reports.
- `job plan` reports the unplaced reason (no node with the devices) as it
  reports any capacity shortfall.

### 6.7 AI6: model-scale cache

- `content.budgets.artifacts` defaults to 50% of the content filesystem with
  a 1 GiB floor and **no ceiling**; snapshots keep 20% clamped 2–200 GiB.
- A budget may be an object `{ share = "60%", max = "4TiB", min = "10GiB" }`
  to cap a share.
- ORAS layers are looked up in and kept in the content store by their layer
  digest (hard links, no copy), like image layers.
- The default per-artifact cap rises from 50 GiB to 1 TiB, and the default
  fetch deadline from 10 minutes to 2 hours, so a 140 GB checkpoint fetches
  with defaults.

### 6.8 PLG1: contained plugin calls

`plugin_runtime::contain(kind, name, op, || call)` runs every call across a
dylib boundary under `catch_unwind`. A panic becomes a typed
`PluginPanicked` error and **quarantines** the plugin: later calls fail fast
with `PluginQuarantined` until the process restarts. Quarantines are listed
by `plugin_runtime::quarantined()`, logged at `error`, and counted in
`legion_plugin_panics_total{kind,name}`. Requires `panic = "unwind"` on both
sides, which the ABI fingerprint already pins.

### 6.9 PLG3: runtime classes

```nickel
container = { runtimeClasses = {
  gvisor = { path = "/usr/local/bin/runsc", userNamespace = false, seccomp = false },
  kata   = { path = "/usr/bin/kata-runtime", userNamespace = false },
} }
```

A task picks one with `config.runtimeClass = "gvisor"`; the worker advertises
`container.runtimeClasses = ["gvisor","kata"]` and the task gets an implicit
`container.runtimeClasses contains "gvisor"` constraint. Preflight runs
`<path> --version`. A class that cannot apply part of the privilege floor
says so in its config; a task whose security block asks for that part is
refused at start with a message naming the class.

### 6.10 PLG4: job rules

```nickel
# legion namespace policy apply -f
{ name = "house-rules", namespaces = ["*"],
  jobRules = [
    { rule = "self.tasks.all(t, !has(t.config.image) || !t.config.image.endsWith(':latest'))",
      message = "pin images by tag or digest, not :latest" },
  ] }
```

Evaluated in `job run` and `job plan` (manager) and `job validate` (when it
can read the policies), refusing with `job rule 'house-rules'[0]: pin images
…`.

### 6.11 PLG5: node checks

Node-check libraries are loaded once per worker and reused (a quarantined
one is dropped). Results are typed: `NodeCheckReport { status, message,
attributes }`.

## 7. Proto, struct, config, and persistence changes

- No new tables or columns. Devices ride `HealthCheckRequest.health_checks`
  under the reserved key `devices`; assignments ride the allocation's
  `resources`.
- `WorkerArtifactsPayload.devices` (worker-type `devices` block).
- `WorkerContainerConfig.runtimeClasses`.
- `QuotaLimit.devices`, `NamespacePolicy.jobRules`.
- `WorkerContentBudgets` accepts the object form.
- Jobspec: `resources.devices`, container `config.runtimeClass` — both in
  `configs/jobs/maximal.ncl`.

## 8. Implementation phases

1. **Devices core** — model, protocol, fake + nvidia plugins, worker
   fingerprint and report, scheduler feasibility and assignment, allocate,
   env, OCI application, quota, visibility, docs. *Built 2026-11-08; not run
   on real GPUs or a real OCI runtime with devices.*
2. **AI6** — budget default and object form, ORAS layer cache, defaults.
   *Built 2026-11-08. P2P, lazy loading, resumable fetches deferred.*
3. **PLG1** — containment and quarantine. *Built 2026-11-08 for
   scheduling, rollout, node-check, and fetcher plugins; task runtime
   dylibs and the WASM move deferred.*
4. **PLG5** — node-check cache and typed reports. *Built 2026-11-08.
   Rollout hooks and controller-side volumes deferred.*
5. **PLG3** — runtime classes. *Built 2026-11-08; not run under gVisor or
   Kata.*
6. **PLG4** — job rules. *Built 2026-11-08. Mutation and a secrets
   provider deferred.*

Each phase ships alone.

## 9. Testing plan

- Unit: protocol parsing, name matching, fingerprint merging, assignment
  (topology, constraints, health), OCI spec application, exec env, quota
  charge, budget sizing, ORAS cache, containment, node-check cache, runtime
  class selection and preflight, CEL job rules.
- Integration (`tests/integration/devices.rs`): the fake plugin end to end —
  advertise, place with 2 devices, assignment recorded, env in the task
  (exec), unhealthy device not schedulable, quota refusal.
- Not run here: real GPUs, real runc/crun with devices, gVisor, Kata.

## 10. Rollout and backward compatibility

No installed base. Nothing changes for jobs that declare no devices. Breaking:
`content.budgets.artifacts` default grows; the node-check plugin contract
changes (`NodeChecker` returns `NodeCheckReport`); the default
`maxArtifactSize` and fetch timeout grow.

## 11. Open questions

- gRPC vs exec for device plugins: exec now (calls #2); revisit if a plugin
  needs streaming health.

## 12. Deferred

- MIG, MPS, time-slicing, fractional devices.
- VFIO passthrough for `sandbox` (Firecracker has none; cloud-hypervisor
  does).
- DRA-style claims shared across tasks.
- Device-aware preemption victim selection and autoscaler node shapes with
  devices.
- Device cgroup enforcement for `exec` (cgroup v2 device control is an eBPF
  program).
- P2P distribution and lazy loading of artifacts.
- WASM for in-manager plugins; out-of-process task runtimes and fetchers.
- Job mutation policies; a secrets provider seam.
- Rollout step/analysis/abort hooks; controller-side volumes.
- YAML CDI specs.

## 13. Acceptance criteria

- A job asking for 2 fake devices places, its allocation records 2 IDs, and
  its `exec` task sees `LEGION_DEVICE_FAKE_GPU_IDS` with them.
- An instance the plugin reports unhealthy is never assigned.
- A namespace quota of N devices refuses the job that would exceed it.
- A container bundle for a device task has the device nodes, allow rules,
  mounts, and env.
- A panicking plugin call returns an error and the plugin is quarantined.
- The artifacts budget on a 1 TiB disk defaults to 512 GiB.
