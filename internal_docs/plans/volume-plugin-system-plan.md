# Plan: Volume Plugin System

> **Judgement calls:** [`calls/volume-plugin-system-plan_calls.md`](../calls/volume-plugin-system-plan_calls.md)

> **Status:** PLANNED — not implemented. This plan is the follow-up that the
> [Stateful Workloads & Instance Identity plan](stateful-workloads-and-instance-identity-plan.md)
> defers to: that plan defines the *binding* (instance → storage handle, attach-before-start,
> detach-on-stop); this plan defines the *drivers* that make a handle real.

## Problem Statement

Stateful instances (slot or keyed) carry a stable `storage_handle` that must be
materialized into a real, mounted path before the task starts, on whatever node the
instance lands on. Today Legion has no storage drivers at all — the handle is an opaque
string and nothing provisions, attaches, or mounts anything.

Hard-coding one storage backend would be the wrong move: storage is the most
environment-specific part of any scheduler (a laptop wants a local directory, a bare-metal
fleet wants ZFS/LVM, a cloud fleet wants EBS/PD via CSI). The right shape is a **volume
plugin system**: a small, stable contract between Legion and external drivers, in the
spirit of CNI's exec-plugin model and Nomad's host/CSI volumes.

## Goals

- **A minimal, versioned plugin contract** covering the full volume lifecycle:
  provision → attach (node) → mount (task) → unmount → detach → destroy, plus
  capability probing.
- **Exec-based plugins as the primary ABI** (CNI-style): a plugin is any executable
  that reads a JSON request on stdin and writes a JSON response on stdout. Language-
  agnostic, crash-isolated, trivially testable from a shell.
- **In-process built-ins for the zero-dependency path**: `local-dir` and `host`
  ship inside the Legion binary so a single-binary cluster gets working stateful
  storage with no external plugins installed.
- **Capability-driven scheduling**: each plugin declares whether its volumes are
  node-local (local) or attachable anywhere (network), and what access modes it
  supports; the scheduler uses this to constrain placement.
- **CSI compatibility layer (optional, later phase)**: a single `csi-bridge` plugin
  that translates the Legion volume contract onto the CSI gRPC services, so the entire
  CSI driver ecosystem (EBS, GCE PD, Ceph, Longhorn, …) is usable without Legion
  implementing per-vendor drivers.
- **End-user simplicity**: a stateful task says *what* it needs (`volume: { plugin:
  'local-dir', size: '10G' }`); operators install plugins; users never see attach/mount
  mechanics.

## Non-Goals

- **Re-implementing CSI inside Legion.** We define a smaller native contract and bridge
  to CSI; Legion core never speaks CSI directly.
- **Snapshots, cloning, expansion in v1.** The contract reserves operation names so
  they can be added without an ABI break, but no v1 plugin implements them.
- **Cross-node data migration.** A node-local volume pins its instance; moving the
  data is the operator's (or a future tool's) job.
- **Networking plugins.** CNI proper (networking) is a separate planned plugin
  category; this plan only borrows CNI's *plugin invocation model*.

---

## Background

### What the stateful plan already defines (the consumer side)

- `Instance.storage_handle: Option<String>` — an opaque ref owned by the instance,
  allocated on first placement, reattached on every later placement.
- Lifecycle hooks: attach-before-start, detach-on-stop, destroy-on-instance-destroy
  (keyed mode) / retain-or-reclaim (slot scale-in).
- Node-affinity feedback into the scheduler: a node-local handle must constrain
  replacement placements to the owning node.

This plan fills in what a `storage_handle` *is*: a structured reference
`{ plugin, volume_id, attrs }` serialized into that field, and the drivers that act on it.

### Prior art we deliberately mirror

| System | What we take from it |
| --- | --- |
| **CNI** | Exec-plugin ABI: JSON on stdin, operation via env/args, JSON result on stdout, versioned contract, plugin discovery from a conf + bin dir. Battle-tested, language-agnostic, and easy to write a plugin for in an afternoon. |
| **Nomad host volumes** | The "operator pre-creates, jobs claim by name" model → our `host` built-in. Also dynamic host volumes (plugin-provisioned node-local dirs) → our `local-dir` built-in. |
| **Nomad/K8s CSI** | Controller vs node split (provision/attach are cluster-level concerns; mount is node-level), access modes, and the idea of a compatibility bridge instead of native vendor drivers. |
| **Legion's own plugin system** ([docs/plugin-system.md](../../docs/plugin-system.md)) | Manifest validation (`plugin.json` with kind/name/api version) and the `plugins/<kind>/<name>/` layout — volume plugins reuse the same discovery conventions under `plugins/volumes/<name>/`. |

> ★ Judgement call ────────────────────────────────
> The request asked for a "CNI compatibility layer." CNI is the *networking* plugin
> standard; its storage-side sibling is **CSI**. We read the intent as: (a) adopt the
> CNI-style exec-plugin *model* for our native ABI, and (b) provide a compatibility
> layer to the existing storage-driver ecosystem — which is CSI. Both are reflected
> above. (Recorded in
> [`calls/volume-plugin-system-plan_calls.md`](../calls/volume-plugin-system-plan_calls.md).)
> ─────────────────────────────────────────────────

---

## Proposed Design

### 1) The volume contract (operations)

Six required operations plus one probe, mirroring the CSI lifecycle but flattened for
simplicity. Every operation is **idempotent** — Legion will retry on worker restarts
and partial failures, so "already done" must be a success, not an error.

| Op | Runs on | Purpose |
| --- | --- | --- |
| `capabilities` | manager + worker | Report contract version, access modes, `node_local` (bool), and which optional ops are supported. |
| `create` | manager (controller) | Provision a volume; returns `volume_id` + attrs. For node-local plugins this may be deferred to first `attach`. |
| `attach` | worker (node) | Make the volume available on this node (e.g. attach an EBS device, ensure a local dir exists). Returns a node-local staging context. |
| `mount` | worker (node) | Expose the attached volume at a concrete path for one allocation; returns `mount_path`. |
| `unmount` | worker (node) | Release the per-allocation mount. |
| `detach` | worker (node) | Release the node attachment (volume survives). |
| `destroy` | manager (controller) | Permanently delete the volume + data (keyed-instance destroy, or slot reclaim when `retain_on_scale_in: false`). |

Reserved (contract names defined, not implemented in v1): `snapshot`, `restore`,
`expand`.

### 2) The exec ABI (CNI-style)

A volume plugin is an executable at `plugins/volumes/<name>/<name>` (or any path on
`LEGION_VOLUME_PLUGIN_PATH`), with a `plugin.json` manifest:

```json
{ "kind": "volumes", "name": "zfs", "legionVolumeApiVersion": 1 }
```

Invocation: Legion runs the binary with the operation as `argv[1]` and a JSON request
on stdin; the plugin writes a JSON response to stdout and exits 0 (success) or non-zero
with `{ "error": { "code", "message", "retryable" } }` on stdout/stderr.

```jsonc
// stdin for: zfs mount
{
  "apiVersion": 1,
  "op": "mount",
  "volume": {
    "id": "vol-9f2c…",
    "plugin": "zfs",
    "attrs": { "dataset": "tank/legion/vol-9f2c" }
  },
  "instance": { "id": "db-default-2", "job": "db", "group": "default", "mode": "slot" },
  "alloc_id": "8841",
  "node": { "id": "node-a1", "data_dir": "/var/lib/legion" },
  "params": { "size": "10G" }            // plugin-specific params from the job spec
}
```

```jsonc
// stdout
{ "apiVersion": 1, "mount_path": "/var/lib/legion/volumes/zfs/vol-9f2c/8841" }
```

Why exec rather than the existing `cdylib` plugin ABI: volume operations are
low-frequency (task start/stop), often need elevated privileges and external CLI tools
(`mount`, `zfs`, cloud SDKs), and must not be able to take down the worker on a crash.
Process isolation buys all three; the per-call spawn cost is irrelevant at this call
rate. The `cdylib` path stays the right answer for hot-path plugins (task kinds,
scheduling policies); volume plugins are deliberately different.

### 3) Built-in plugins (ship with Legion, in-process)

Built-ins implement the same Rust trait the exec dispatcher does (`VolumePlugin`), so
the rest of the system is agnostic to whether a plugin is in-process or exec'd.

1. **`local-dir`** *(the default)* — dynamically provisions a directory under
   `<data_dir>/volumes/local-dir/<volume_id>` on the node of first placement.
   `node_local: true`: the scheduler pins the instance to that node thereafter.
   `destroy` removes the directory. Zero configuration; this is what a bare
   `stateful: { storage: { path: … } }` block uses when no plugin is named.
2. **`host`** — Nomad-host-volume style: the operator declares named host paths in the
   worker config (`worker.host_volumes: { models: '/mnt/models' }`); jobs claim them by
   name. No provisioning; `create`/`destroy` are no-ops; `node_local: true`; supports
   read-only mounts. The escape hatch for "the data is already on the machine."

Suggested early exec plugins (separate small repos / `plugins/volumes/`, not blocking
v1): `nfs` (mount an export — first *non*-node-local plugin, exercises free
rescheduling), `zfs` (datasets + future snapshot support).

### 4) CSI compatibility layer (optional phase)

One exec plugin, `csi-bridge`, translates the Legion contract onto a CSI driver's
gRPC endpoints:

- `create`/`destroy` → CSI `CreateVolume`/`DeleteVolume` (Controller service)
- `attach` → `ControllerPublishVolume` + `NodeStageVolume`
- `mount`/`unmount` → `NodePublishVolume`/`NodeUnpublishVolume`
- `detach` → `NodeUnstageVolume` + `ControllerUnpublishVolume`
- `capabilities` → `GetPluginCapabilities`/`ValidateVolumeCapabilities`

Configured per-driver: `params: { csi_endpoint: 'unix:///run/csi/ebs.sock', … }`.
This keeps CSI's considerable complexity (staging vs publishing, controller RPCs,
idempotency rules — which CSI also mandates, making the mapping natural) inside one
plugin instead of inside Legion core. CSI drivers that require a kubelet-style
registration loop may need a thin sidecar; documented per-driver, out of scope here.

### 5) Job spec (user-facing)

The `stateful.storage` block from the stateful plan grows a `plugin` + `params`:

```jsonnet
stateful: {
  mode: 'slot',
  storage: {
    path: '/var/lib/db',          // where the task sees it (mount target inside task fs)
    plugin: 'local-dir',          // default if omitted
    params: { size: '10G' },      // passed through to the plugin verbatim
    retain_on_scale_in: true,
  },
},
```

Claiming an operator-defined host volume:

```jsonnet
storage: { path: '/models', plugin: 'host', params: { name: 'models', read_only: true } }
```

Validation at admission: named plugin must be installed (manager probes
`capabilities`), `params` must satisfy the plugin's declared required params, and
access-mode conflicts (two writers on a single-writer volume) are rejected.

### 6) Scheduler integration

- `capabilities.node_local: true` → after first placement, the instance record pins
  `node_id`; the reconciler only places replacements on that node and surfaces a clear
  diagnostic (`volume is node-local; waiting for node <id>`) if it's gone.
- `node_local: false` → no constraint; reschedule anywhere, `attach` follows.
- Worker fingerprinting advertises installed volume plugins (name + version) the same
  way task-kind support is advertised, so placement can require `plugin ∈ node.volume_plugins`.

### 7) State & failure model

- `volumes` table: `{ volume_id, plugin, instance_id, state, node_id?, attrs_json,
  created_at }`, `state ∈ { creating, created, attached, mounted, destroying, destroyed }`.
- All transitions are manager-driven and idempotent; a crashed worker re-runs
  `attach`+`mount` on restart (safe by the idempotency rule).
- Orphan reconciliation: a background loop destroys volumes whose instance is
  `Terminated` (keyed) or reclaims slot volumes per `retain_on_scale_in`.
- `destroy` is **two-phase**: mark `destroying` first, then call the plugin, then mark
  `destroyed` — so a crash mid-destroy is retried, never leaked.

---

## Implementation Phases

1. **Contract + trait + built-ins** — `VolumePlugin` trait, `local-dir` and `host`
   in-process plugins, `volumes` table, wire into the stateful attach/detach hooks.
   *Exit:* a slot-mode job gets a persistent local dir that survives task restarts on
   the same node.
2. **Exec dispatcher** — manifest discovery, JSON ABI, timeout + retry policy, a
   `legion volume plugin test <name>` CLI for plugin authors (runs the op cycle against
   a scratch volume).
3. **Scheduler affinity + fingerprinting** — node pinning for affine volumes, plugin
   advertisement, placement constraints.
4. **Reference exec plugin (`nfs`)** — proves the non-affine path end to end.
5. **CSI bridge** *(optional)* — the `csi-bridge` exec plugin + per-driver docs.
6. **Reserved ops** *(future)* — snapshot/restore/expand.

## Testing Plan

- **Contract tests** (reusable harness): run every plugin — built-in or exec — through
  create→attach→mount→write file→unmount→detach→attach→mount→read file→destroy, plus
  double-call idempotency on every op. The same harness backs the
  `legion volume plugin test` CLI.
- **Unit:** handle (de)serialization, manifest validation, exec ABI framing (bad JSON,
  non-zero exit, timeout), capability parsing, admission validation errors.
- **Integration:** slot reschedule on the same node reattaches the same `local-dir`
  volume with data intact; keyed destroy removes the volume; `retain_on_scale_in`
  honored both ways; node-local instance blocks placement when its node is down and
  reports the diagnostic.
- **Regression:** stateless jobs and stateful jobs predating volumes (bare `path`)
  behave unchanged.

## Open Questions

1. Should `create` for node-local plugins be lazy (at first attach) always, or should
   the contract have an explicit `topology` hint so the scheduler picks the node first?
   (Leaning: lazy-create on first attach; simplest and matches local-dir reality.)
2. Volume sharing across instances (many-readers): defer entirely, or reserve
   `access_mode` in the spec now? (Leaning: reserve the field, reject ≠ single-writer.)
3. Who runs `destroy` for node-local volumes when the node is permanently gone?
   (Likely: operator-forced `legion volume delete --force`, marking state without
   plugin confirmation.)
4. Exec plugin privileges: run as the worker's user always, or support a per-plugin
   `run_as` in the manifest? (Mount operations often need root; workers often run as root
   anyway — start with worker-user, revisit.)
