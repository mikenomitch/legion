# Devices: GPUs and Other Hardware

A task can ask for whole devices — GPUs, FPGAs, NICs, anything a node has a
countable number of — and get exactly the ones it was assigned: as ids in its
environment, and, for a container, as device nodes, cgroup rules, mounts, and
hooks in its OCI bundle. Devices are discovered by **device plugins** (small
programs that speak the `legion-device/1` protocol over JSON), or declared by
hand. Their health is re-checked every 30 seconds, and an unhealthy device is
never assigned.

```nickel
# A job: two NVIDIA GPUs with at least 40 GB each.
resources = {
  memory = 32768,
  devices = [
    { name = "nvidia/gpu", count = 2,
      constraints = [{ key = "memoryMiB", operator = ">=", value = 40000 }] },
  ],
},
```

```nickel
# A worker type: discover GPUs with the reference nvidia plugin.
devices = {
  plugins = {
    nvidia = { command = ["/opt/legion/plugins/devices/nvidia/legion-device-nvidia"] },
  },
},
```

The task then sees:

```text
LEGION_DEVICE_NVIDIA_GPU_IDS=GPU-6f2c…,GPU-91ab…
CUDA_VISIBLE_DEVICES=GPU-6f2c…,GPU-91ab…
NVIDIA_VISIBLE_DEVICES=GPU-6f2c…,GPU-91ab…
```

## The model

A node advertises **device groups**. A group is a set of identical devices
from one source, named `<vendor>/<type>/<model>`:

```json
{
  "vendor": "nvidia", "type": "gpu", "model": "A100-SXM4-80GB",
  "plugin": "nvidia",
  "attributes": { "memoryMiB": 81920, "driver": "550.54.15", "computeCapability": "8.0" },
  "instances": [
    { "id": "GPU-6f2c…", "healthy": true,
      "attributes": { "index": 0, "pciBusId": "00000000:07:00.0", "numaNode": 0, "peerGroup": "nvlink-0" } },
    { "id": "GPU-91ab…", "healthy": false, "healthDetail": "uncorrected ECC errors: 3",
      "attributes": { "index": 1, "pciBusId": "00000000:0F:00.0", "numaNode": 0, "peerGroup": "nvlink-0" } }
  ]
}
```

- **Instances** have an opaque `id` (unique within the group), a `healthy`
  bit with a `healthDetail`, and attributes.
- **Attributes** are free-form. The group's apply to every instance; an
  instance's own win. Two instance attributes have a meaning to Legion:
  `peerGroup` (devices joined by a fast link, such as an NVLink domain) and
  `numaNode`. See [Assignment](#assignment).
- **`cdi`** on an instance names its device in a [CDI](#cdi-specs) spec
  (`nvidia.com/gpu=GPU-6f2c…`).

`legion node info <node>` shows every group and instance with its health,
and any plugin whose last fingerprint failed:

```console
$ legion node info gpu-07
Node ID: gpu-07
...
Devices:
  nvidia/gpu/A100-SXM4-80GB  (7 healthy of 8, plugin nvidia)  {"computeCapability":"8.0","driver":"550.54.15","memoryMiB":81920}
    GPU-6f2c…  healthy  {"index":0,"numaNode":0,"pciBusId":"00000000:07:00.0","peerGroup":"nvlink-0"}
    GPU-91ab…  UNHEALTHY: uncorrected ECC errors: 3  {"index":1,...}
    ...
```

With `-j`, the same report is at `healthChecks.devices` of the node JSON
(`{ "groups": [...], "plugins": { "<name>": { "status", "error", "groups" } } }`).
The MCP tool `legion_node_devices` returns that report for one node or for
every node that has devices.

## Asking for devices

`resources.devices` is a list of requests:

| Field | Default | Meaning |
| --- | --- | --- |
| `name` | required | `type` (`gpu`), `vendor/type` (`nvidia/gpu`), or `vendor/type/model` (`nvidia/gpu/A100-SXM4-80GB`). Matches every group whose named parts are equal. |
| `count` | `1` | How many instances, 1 to 1024. All come from **one group** (one model). |
| `constraints` | none | The [task constraint syntax](jobs-tasks-and-scheduling.md), evaluated per instance over its attributes plus `vendor`, `type`, `model`, and `id`. |
| `affinities` | none | The task affinity syntax over the same attributes: which instances to prefer on the chosen node. |

A task may make up to 16 requests. Two requests may not share a name, or
names that map to the same environment variable (`nvidia/gpu` and
`nvidia-gpu`). Submit refuses a request with an `assigned` field: that is
the scheduler's.

Each request's ids reach the task as `LEGION_DEVICE_<NAME>_IDS`, comma
separated, where `<NAME>` is the request's `name` upper-cased with every
character that is not a letter or digit as `_` (`nvidia/gpu` →
`LEGION_DEVICE_NVIDIA_GPU_IDS`). Whatever env the device plugin adds comes
too.

### Where it places

A node fits a task when, for every request, some matching group has `count`
free, healthy instances that satisfy the request's constraints, alongside the
task's cpu, memory, and the rest. Device requests are a fit check only: they
do not change how nodes are scored, and **affinities choose devices on the
chosen node, not the node**. A job that cannot place says so in `job plan`
and `job run`, like any capacity shortfall.

### Assignment

The scheduler chooses a node; the exact instances are chosen when the
allocation is written, inside the same transaction, against the node's
devices as committed at that moment, so two allocations can never be handed
one device. The ids are recorded on the allocation itself:

```json
"resources": {
  "memory": 32768,
  "devices": [{
    "name": "nvidia/gpu", "count": 2,
    "assigned": { "device": "nvidia/gpu/A100-SXM4-80GB", "plugin": "nvidia",
                  "ids": ["GPU-6f2c…", "GPU-0a77…"] }
  }]
}
```

`legion ledger` (and `-j`) prints every allocation's resources per node,
assignments included, which is how to see which job holds which GPU.

Within a group, in order of preference:

1. **Affinities**: instances that match the request's affinities more.
2. **Topology**: all instances inside one `peerGroup` (an NVLink domain),
   else inside one `numaNode`, else anywhere in the group.
3. **Best fit**: of the domains big enough, the smallest, so a later
   eight-GPU job still finds a whole domain.
4. Instance order.

Between groups (a request for `gpu` on a node with A100s and T4s),
affinities, then topology, then best fit pick the group.

## Device plugins

A device plugin is any command. The worker runs it once per operation with
one JSON request on stdin and reads one JSON reply from stdout. A non-zero
exit, a reply that is not JSON, or running past `timeout` (default `10s`) is
an error. Every request carries `protocol` (`legion-device/1`), `op`,
`plugin` (its name in the worker type), and `config` (its `config` record).

### `fingerprint`

Run when the worker starts (the report rides its registration) and on every
node-check pass (every 30 seconds).

```json
{"protocol":"legion-device/1","op":"fingerprint","plugin":"nvidia","config":{}}
```

```json
{"devices":[ <group>, ... ]}
```

Groups as in [The model](#the-model), without `plugin` (the worker sets it).
A group needs a `vendor`, `type`, and `model` with no `/`, `,`, `=` or
surrounding spaces, and unique instance ids. A device that is gone is left
out; a device that is present but broken is `"healthy": false` with a
`healthDetail`. A plugin that fails reports no groups until it succeeds
again, and its error shows in `legion node info`.

Keep the reply stable: the worker sends the report on every pass, and the
manager only writes it when it changed. A plugin whose output changes every
pass (a timestamp, a temperature) writes the node every 30 seconds.

### `allocate`

Run at task start, once per request, before the runtime prepares the task.

```json
{"protocol":"legion-device/1","op":"allocate","plugin":"nvidia","config":{},
 "device":"nvidia/gpu/A100-SXM4-80GB","ids":["GPU-6f2c…","GPU-0a77…"]}
```

```json
{
  "containerEdits": {
    "env": ["CUDA_VISIBLE_DEVICES=GPU-6f2c…,GPU-0a77…"],
    "deviceNodes": [{ "path": "/dev/nvidia0" }, { "path": "/dev/nvidiactl" }],
    "mounts": [{ "hostPath": "/usr/lib/x86_64-linux-gnu/libcuda.so.1",
                 "containerPath": "/usr/lib/x86_64-linux-gnu/libcuda.so.1",
                 "options": ["ro", "nosuid", "nodev", "bind"] }],
    "hooks": [{ "hookName": "createContainer", "path": "/usr/bin/nvidia-ctk",
                "args": ["nvidia-ctk", "hook", "update-ldcache"] }]
  },
  "cdiDevices": ["nvidia.com/gpu=GPU-6f2c…"]
}
```

`containerEdits` is CDI's `containerEdits`, field for field: `env` entries,
`deviceNodes` (`path`, optional `hostPath`, `type` `c`/`b`, `major`, `minor`,
`fileMode`, `permissions` for the cgroup rule, default `rwm`, `uid`, `gid`;
type, major, and minor are read from the host node when left out), `mounts`
(`hostPath`, `containerPath`, `type`, `options`; default options `rbind,
rprivate`), and `hooks` (`hookName` one of `prestart`, `createRuntime`,
`createContainer`, `startContainer`, `poststart`, `poststop`). Paths must be
absolute. `cdiDevices` names CDI devices whose edits Legion adds (see below).
Both are optional; a statically declared group has no plugin and gets only
its CDI edits and the `LEGION_DEVICE_*` variable.

An allocate that fails fails the allocation, with the plugin's error, and the
manager reschedules it.

### Testing a plugin

```console
$ legion device plugin test -- /opt/legion/plugins/devices/nvidia/legion-device-nvidia
Device plugin conformance: plugin (legion-device/1)

  ✓ fingerprint                      1 group(s), 8 instance(s)
  ✓ fingerprint is stable            two fingerprints agree
  ✓ allocate nvidia/gpu/A100-SXM4-80GB 2 env, 3 device node(s), 0 mount(s), 0 hook(s), 0 CDI device(s)

PASS — plugin speaks legion-device/1.
```

`--config '<json>'` passes the plugin's `config`, `--timeout` its timeout,
and `-j` prints the checks as JSON. It exits non-zero when a check fails.

### Shipped plugins

- **`plugins/devices/nvidia/legion-device-nvidia`**: wraps `nvidia-smi`. One
  group per GPU model (`nvidia/gpu/<model>`, with `NVIDIA ` dropped and
  spaces as `-`), one instance per GPU with its UUID as the id, and
  `index`, `pciBusId`, `numaNode` (from sysfs), and `peerGroup` (GPUs joined
  by NVLink in `nvidia-smi topo -m`). A GPU with uncorrected volatile ECC
  errors is unhealthy. `allocate` returns `NVIDIA_VISIBLE_DEVICES`,
  `CUDA_VISIBLE_DEVICES`, and the device nodes. With `--cdi=index` or
  `--cdi=uuid` it also names the GPUs' CDI devices, which a container needs
  for the driver libraries: generate the spec with
  `nvidia-ctk cdi generate --format=json --output=/etc/cdi/nvidia.json`
  (with `--device-name-strategy=uuid` for `--cdi=uuid`).
- **`plugins/devices/fake/legion-device-fake <inventory.json>`**: reports the
  groups in a file, and allocates `/dev/null` with `FAKE_VISIBLE_DEVICES`.
  Edit the file to simulate a failure. Legion's own tests use it.

## Worker configuration

```nickel
devices = {
  # Plugins, by name. The name is what assignments record.
  plugins = {
    nvidia = {
      command = ["/opt/legion/plugins/devices/nvidia/legion-device-nvidia", "--cdi=uuid"],
      config = {},          # passed through as `config`
      timeout = "10s",      # per call
    },
  },
  # Devices declared by hand. Always healthy unless declared otherwise.
  static = [
    { vendor = "xilinx", type = "fpga", model = "u250",
      attributes = { memoryMiB = 65536 },
      instances = [{ id = "0", cdi = "xilinx.com/fpga=0" }, { id = "1", cdi = "xilinx.com/fpga=1" }] },
  ],
  # Where CDI specs are read. This is the default.
  cdiSpecDirs = ["/etc/cdi", "/var/run/cdi"],
},
```

The manager validates the block when it loads its worker types: plugin names
are letters, digits, `-` and `_` (`static` is reserved), every plugin has a
command, static groups are well formed with unique ids, and the CDI dirs are
absolute. A worker type's `resources` may not declare `devices` or
`device:*` keys: devices are declared here.

## CDI specs

Legion reads [CDI](https://github.com/cncf-tags/container-device-interface)
spec files from `cdiSpecDirs`. A device named by an instance's `cdi` field or
by a plugin's `cdiDevices` reply (`<kind>=<name>`, for example
`nvidia.com/gpu=0`) gets its spec's top-level `containerEdits` once and its
own `containerEdits`. Only **JSON** spec files (`*.json`) are read; a YAML
spec is ignored. A name no spec defines fails the task's start with the dirs
it searched.

## What each runtime does with devices

| Runtime | Env (`LEGION_DEVICE_*`, plugin env) | Device nodes and cgroup rules | Mounts and hooks |
| --- | --- | --- | --- |
| `container` (Linux, runc/crun) | Yes | Yes: `linux.devices`, and an allow rule per device in `linux.resources.devices` after the deny-all rule | Yes |
| `exec` | Yes | No: the process sees every device on the host, and the ids are what it should use | No (it shares the host filesystem) |
| `noop` and dylib runtimes | In `TaskInstance.extra_env` | Up to the runtime (`TaskInstance.devices` carries the edits) | Up to the runtime |
| `sandbox` | Refused at submit | — | — |
| `container` on macOS (Apple `container`) | No | No | No |

## Quota

A [namespace policy](namespaces.md) caps devices per band by name:

```nickel
quota = { normal = { cpu = 64000, devices = { "nvidia/gpu" = 8, "fpga" = 2 } } }
```

A job is charged, like cpu and memory, the larger of what it declares (each
count-driven task's count times its requests) and what its live allocations
hold. A request counts against every quota key it could be served under:
`gpu` counts against `nvidia/gpu` (an NVIDIA card could serve it), `amd/gpu`
does not. Over the cap, the submit fails:

```text
namespace policy 'ml-teams' caps band 'normal' at 8 nvidia/gpu devices across ml-*; in use 8, this job adds 2
```

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| Submit: `resources.devices[0].count must be between 1 and 1024` | A zero or huge count | Fix the count |
| Submit: `device name "a/b/c/d" must be type, vendor/type, or vendor/type/model` | A malformed name | Use one to three parts |
| Submit: `task … asks for resources.devices, but sandbox tasks cannot use devices yet` | A sandbox task with devices | Run it as `container` |
| The job does not place | No node has enough free, healthy, matching instances | `legion node info` on the nodes you expect; check `healthDetail` and the request's constraints |
| `node info` shows `plugin nvidia: error …` | The plugin failed its last fingerprint (missing `nvidia-smi`, a timeout) | Fix the plugin; its groups come back on the next pass |
| Allocation fails: `failed to allocate the devices of task …` | The plugin's `allocate` failed, or a CDI device is in no spec | The message carries the plugin's error |
| Allocation fails: `device request … has no assignment` | Work that did not come through the scheduler (an edge start) asked for devices | Devices are only for scheduled work |
| A container task cannot open a device | The image lacks the user-space driver libraries | Use `--cdi=…` with a generated CDI spec, or an image that ships them |

## Health

The worker re-runs every plugin's `fingerprint` on each node-check pass (30
seconds) and sends the result with its health checks. An instance that turns
unhealthy, or disappears, stops being assigned from that moment. Running
allocations that hold it are **not** stopped: Legion cannot tell whether the
task still uses it, and a GPU with a recoverable error often is. Stop or
reschedule them yourself. A repaired or added device makes the manager
re-check jobs that are waiting for capacity.

An unhealthy device does not make the node unhealthy.

## Limitations

These are not implemented yet:

- **No sharing**: a device is assigned whole. MIG partitions, MPS, and time
  slicing are not modeled (a plugin can report MIG instances as their own
  group with their own ids, and Legion will assign them whole).
- **No microVM passthrough**: `sandbox` tasks cannot use devices (no VFIO).
- **No device cgroup for `exec`**: an `exec` task can open any device node;
  the assignment tells it which to use, it does not stop it using others.
- **Device topology is per node**: assignment keeps one allocation's
  devices inside one NVLink/peer group and NUMA node, but device attributes
  are not node attributes, so a gang cannot be kept inside an NVLink domain
  that spans nodes. A gang *can* be kept inside one rack or other
  node-attribute domain with [`gang.topology`](gang-scheduling.md#topology).
- **Preemption ignores devices** when choosing victims, and the autoscaler's
  node shapes do not model devices.
- **YAML CDI specs** are not read.
- **Edge-started work** (boot tickets) cannot use devices.

## What has been verified

The plugin protocol, assignment, quota, the env an `exec` task gets, and the
OCI spec Legion generates are covered by tests using the fake plugin. The
reference nvidia plugin's parsing is tested against a stub `nvidia-smi`.
**None of it has run on real GPU hardware, and the generated device entries
have not been run under a real runc or crun.** Treat the nvidia plugin as a
reference to adapt, and report what you find.
