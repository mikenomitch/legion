# Judgement Calls: Device Model, Model-Scale Artifacts, and Plugin Seams

Plan: [device-model-and-plugin-seams-plan.md](../plans/device-model-and-plugin-seams-plan.md).
Covers review findings PLG1–PLG5 and AI6.

Entries are ordered by project impact, highest first; an entry's number is its
rank.

## 1. A device assignment is written into the allocation row, at insert, inside the apply transaction

**Call.** The scheduler only decides *whether* a node fits a device request.
The concrete instance ids are chosen in `Store::insert_allocation_with_data`
(the one choke point every placement path writes through), against the node
row and the allocation rows the same transaction sees, and recorded in the
row's own `resources`:

```json
"devices": [{ "name": "nvidia/gpu", "count": 2,
              "assigned": { "device": "nvidia/gpu/A100-SXM4-80GB", "plugin": "nvidia",
                            "ids": ["GPU-6f2c…", "GPU-0a77…"] } }]
```

A node whose devices went since the plan snapshot fails the insert with a
typed `DevicesUnavailable`, and the apply walk moves to the next candidate,
the way it does for a static port. Usage, quota, the worker, and `node
usage` all read the row.

**Alternatives.** (a) Choose ids at plan time and carry them in the plan
submission (Nomad assigns device instances in its plan). (b) A separate
`device_assignment` table keyed by allocation.

**Trade-off.** Assignment costs one extra read of the node's live allocation
rows per device-bearing placement inside the serialized apply transaction.
Free for jobs without devices, and device jobs are rare and slow, but a
cluster placing thousands of GPU tasks per second would feel it. The planner's
ledger has to guess the same ids (it calls the same function on its own pool)
to keep later placements in one plan honest; when it guesses differently, the
apply's re-check catches it.

**Why (simplicity, correctness).** Two allocations can never be handed one
device, because the choice and the write are one transaction against
committed rows; a plan-time choice would need every apply to re-validate ids
anyway. Writing the ids into `resources` means the existing item arithmetic
(free pools, the cached usage aggregates, preemption release, suspend holds)
counts devices with no new code path, and there is no new table, migration,
or replication lane to reason about.

## 2. The device report rides the node-check health report, not a new RPC, column, or fingerprint field

**Call.** The worker sends its device report as `healthChecks.devices` on
the existing `HealthCheck` RPC every 30 seconds, and seeds it at
registration. `ResourcePool::from_node` reads it; `derive_node_health` and the
UI's health badge skip the key, so an unhealthy GPU is not an unhealthy node.

**Alternatives.** (a) `fingerprint_attrs.devices` (where static facts live)
updated by a new worker-plane `ReportNodeDevices` RPC, or by re-registering
on change. (b) A `devices` column on `node_health`.

**Trade-off.** Devices live under a key named "health checks", and every
reader of health checks has to know to skip it (two places did). A reviewer
reading the node JSON finds GPUs in an odd place; the docs say where.

**Why (performance, simplicity).** The health-check write is status-lane,
forwarded to the lane leader when a worker talks to a follower, and compared
before it is written, so an unchanged report costs a read and nothing else.
A control-lane `fingerprint_attrs` update would not be forwarded (follower
workers could never report a GPU failing), a new column would change the
`Node` struct that dozens of literals construct, and re-registering on every
health flip emits registration events and resets liveness. Device health
genuinely is health; the inventory changing with it (a card falling off the
bus) is part of the same report.

## 3. Plugin panic containment is done inside the plugin, with a C-ABI probe and a quarantine

**Call.** `legion::export_*!` macros wrap each scheduling, rollout,
node-check, and fetcher plugin in `Contained<T>`, whose generic trait impls
are compiled into the plugin and run each method under the *plugin's*
`catch_unwind`, recording the panic. After every call the host's `guard()`
asks the plugin through `legion_plugin_take_panic` and, on a panic,
quarantines the plugin until the process restarts and returns
`PluginPanicked`. The host refuses a plugin that exports only the old `get`.

**Alternatives.** (a) Wrap host-side calls in `catch_unwind`, as the review
suggested. (b) Move in-manager plugins onto the WASM layer now. (c) Run them
out of process.

**Trade-off.** Every in-tree plugin and the docs changed (plugin API version
3), and a plugin author must use the macro; a panic in the exported `extern
"C"` constructor itself still aborts. A quarantine is process-wide and lasts
until restart, so a plugin that panicked once on a weird input stays off.

**Why (extensibility, correctness).** (a) does not work: a `cdylib` carries
its own copy of `std`, and a panic unwinding into the host is a foreign
exception that the host's `catch_unwind` turns into an abort (current
`panic_unwind` checks the exception's canary). Containment has to happen on
the plugin's side of the boundary. (b) and (c) are the right long-term homes
(TODO), but each is a project; this makes today's plugins safe now. Quarantine
rather than retry because a panicking plugin is a bug, and calling it again on
the scheduler's hot path invites the same panic every eval.

## 4. Device plugins are exec + JSON (`legion-device/1`), not gRPC sockets or dylibs

**Call.** A device plugin is any command, run once per operation with one
JSON request on stdin and one reply on stdout (`fingerprint`, `allocate`),
configured by command in the worker type's `devices.plugins`, the convention
the keyring and cluster-CA `exec` providers use.

**Alternatives.** (a) Kubernetes' model: a long-running gRPC server on a unix
socket with a streaming `ListAndWatch`. (b) A dylib trait like task runtimes.

**Trade-off.** No push: health changes are noticed on the next 30-second
pass, not the instant the driver reports them, and every pass forks the
plugin (and `nvidia-smi`). A plugin cannot keep state between calls except on
disk.

**Why (simplicity, extensibility).** Device plugins shell out to vendor tools
anyway, run at a low rate, and must not take the worker down when the vendor
tool hangs or crashes. A reference plugin fits in a shell script any operator
can read and adapt (`plugins/devices/nvidia`), and `legion device plugin test`
exercises it with no cluster. gRPC can come later behind the same request and
reply shapes if a plugin needs streaming health.

## 5. The artifacts cache defaults to half the disk with no ceiling, and bounds are an object

**Call.** `content.budgets.artifacts` defaults to 50% of the content
filesystem, at least 1 GiB, with no ceiling (was 10% clamped to 1–20 GiB). An
operator bounds a share with `{ share = "50%", max = "2TiB", min = "10GiB" }`.

**Alternatives.** (a) A share of *free* space at worker start. (b) Keep a
ceiling, raised to something like 500 GiB.

**Trade-off.** On a node with a big data disk the cache can now take half of
it, and on small disks the default share is larger than before (4 GiB of an
8 GiB disk), leaving less for volumes and checkpoints unless the operator
sets a budget. `minFreeDisk` still evicts and then refuses before the disk
fills. An operator who had set `snapshots = "60%"` now fails validation (50%
plus 60%), with a message saying so.

**Why (performance for inference).** One 70B checkpoint is around 140 GB; any
fixed ceiling in the tens of GiB means every restart re-downloads the model.
A free-space share (a) changes meaning every restart and with every other
tenant of the disk. Breaking the default is allowed (no installed base), and
an object with named bounds reads better than a mini-language in a string.

## 6. Container edits are CDI's `containerEdits`, verbatim; only JSON CDI specs are read

**Call.** A plugin's `allocate` returns CDI `containerEdits` (env, device
nodes, mounts, hooks) and optional `cdiDevices`, which Legion resolves from
`/etc/cdi` and `/var/run/cdi`. Static devices name their CDI device with
`cdi`. Legion fills a device node's type and numbers from the host when they
are left out.

**Alternatives.** (a) A Legion-specific reply shape. (b) Full CDI including
YAML specs (a `serde_yaml` dependency).

**Trade-off.** YAML specs (nvidia-ctk's default output) are ignored; the docs
say to generate JSON. CDI fields Legion does not apply (`additionalGIDs`,
`intelRdt`) are not parsed.

**Why (extensibility).** Reusing CDI means vendors' existing specs work, and
the NVIDIA container toolkit's library mounts and ldcache hook come for free
with `--cdi=...`. YAML would add a direct dependency (and a lockfile change on
a branch merged with five others) for a format `nvidia-ctk` can already write
as JSON.

## 7. Runtime classes adapt the privilege floor only under `isolatesRoot`; otherwise they refuse

**Call.** A runtime class declares whether it supports a user namespace and
seccomp, and whether it keeps root off the host by itself (`isolatesRoot`).
A floor part the class cannot apply is dropped when `isolatesRoot`, and the
start is refused otherwise. Submit-time host-level classification is
unchanged.

**Alternatives.** (a) Require tasks to set `security.userNamespace = false`
under gVisor or Kata. (b) Drop unsupported parts silently for every class.

**Trade-off.** `isolatesRoot` is an operator trust statement Legion cannot
verify; an operator who sets it on a plain runc wrapper runs container root
as host root without the task being host-level.

**Why (simplicity, security).** (a) would make every gVisor task host-level
(an opt-out of the floor is host-level by definition), so running untrusted
code in a sandbox would need *more* privilege than running it in plain runc,
the opposite of the point. (b) would quietly run tasks below the floor they
asked for. Naming the trust explicitly in the class keeps the decision where
the operator can see it.

## 8. Job rules live in namespace policies, not manager config

**Call.** CEL `jobRules` are a field of namespace policies; a policy with
`namespaces = ["*"]` is the cluster-wide form. They run inside `admit_job`
(so `job run`, `job plan`, and `RunJobTemplate`) with object rules' cost
bounds, and `oldSelf` is refused.

**Alternatives.** (a) A cluster-wide list in manager config, plus namespace
policies. (b) A new object kind (`JobPolicy`).

**Trade-off.** A cluster-wide rule is a policy object that has to be applied,
not a line in the manager's config file, and `*` does not reach
`legion-system` (by the reserved-prefix rule), so Legion's own jobs are not
governed by tenant house rules.

**Why (simplicity).** Namespace policies already are the "what jobs here may
do" object, written with `acl:write` and previewed with `--dry-run`, which now
lists existing jobs a new rule would refuse. A second place for the same
concern would need its own precedence rules; one place has none.

## 9. A quarantined scheduling plugin places with `bin_pack`

**Call.** When a scheduling plugin panics or is quarantined, the host's
`GuardedPolicy` answers with `bin_pack` for that placement and every later
one.

**Alternatives.** Fail the placement (the plugin chose nothing).

**Trade-off.** The cluster silently keeps placing with a policy the operator
did not choose, until someone reads the error log or
`legion_plugin_panics_total`.

**Why (simplicity at 3am).** A scheduling plugin bug would otherwise stop
every placement in the cluster; degraded placement quality is the smaller
incident. The counter and the `error` log make it visible.

## 10. One request is served from one group, inside one topology domain when it fits

**Call.** All instances of a request come from one device group (one model).
Within a group, Legion keeps the request inside one `peerGroup` (NVLink
domain), else one `numaNode`, preferring the job's affinities first and the
smallest sufficient domain (best fit) last. Between groups: affinities, then
locality, then best fit.

**Alternatives.** (a) Mix models within a request. (b) Kubernetes' Topology
Manager policies (`restricted`, `single-numa-node`) as a knob.

**Trade-off.** A request that could be met by mixing an A100 and an H100 does
not place. Topology is a preference, not a constraint: a request that fits no
single domain still places, spread (a job that must not spread says so with a
`constraints` rule on `peerGroup`, which a single request cannot express
today).

**Why (simplicity).** Mixed models in one training job are almost always a
mistake; Nomad makes the same rule. A knob for topology policy is something
nobody tunes; best-effort packing gets the common case (2 or 4 GPUs on one
NVLink island) right with nothing to configure.

## 11. Device constraints reuse the task constraint syntax; affinities choose devices, not nodes

**Call.** `resources.devices[].constraints` and `affinities` are the task's
own syntax, evaluated over the device's attributes plus `vendor`, `type`,
`model`, and `id`. Affinities pick instances on the chosen node; node scoring
is unchanged.

**Alternatives.** Feed device affinity into node scoring, as Nomad does.

**Trade-off.** A job that prefers H100s will take A100s on the first node the
scheduler picks even if another node had H100s free.

**Why.** Another engineer is reworking `bin_pack`/`spread` scoring in this
same change (SCH4); keeping devices in feasibility and assignment keeps the
two from colliding, and a constraint expresses "must be H100".

## 12. Quota charges a request against every key it could be served under

**Call.** `quota.<band>.devices = { "nvidia/gpu" = 8 }` charges declared
requests by name compatibility (`gpu` counts against `nvidia/gpu`) and live
allocations by their assigned group.

**Alternatives.** Charge only exact name matches.

**Trade-off.** A request for `gpu` on a cluster with only AMD GPUs is still
charged against an `nvidia/gpu` cap.

**Why (security).** Exact matching would let a tenant escape an `nvidia/gpu`
cap by writing `gpu`.

## 13. A device that turns unhealthy is not taken away from running allocations

**Call.** An unhealthy instance stops being assigned; allocations that hold
it keep running.

**Alternatives.** Stop or reschedule allocations whose device went unhealthy
(Kubernetes leaves the pod; Nomad can reschedule).

**Trade-off.** A job on a dead GPU keeps running (and failing) until its own
health checks or an operator act.

**Why.** Legion cannot tell whether the task still uses the device, and many
GPU errors are recoverable. Killing a week-long training run on a transient
ECC report is worse than leaving it.

## 14. ORAS layers are copied in and out of the content store, never hard-linked

**Call.** An ORAS layer hit is copied into the task's directory; a miss is
downloaded and a copy kept.

**Alternatives.** Hard links both ways, as image layers do.

**Trade-off.** Each allocation costs a local copy of the weights (an
in-kernel copy, a reflink where the filesystem offers one) and transiently
twice the disk.

**Why (correctness).** Image layers are extracted into a rootfs and never seen
by the task; ORAS layers are the task's own files. With a shared inode a root
task that writes to its weights would change what the next allocation gets,
and the store's cheap `get` (size only) would not notice. Keeping "tasks get
their own copy" matches single-file artifacts.

## 15. Cached ORAS layers follow the image cache's repository rule

**Call.** A cached ORAS layer is served only to a pull from the repository
(`registry/repo`) it was downloaded and verified in. Each layer's source is
recorded when it is stored, and a pull is never served a layer by digest
alone. This came up when this branch was merged with `main`'s private image
cache fix (#67), which scopes cached image blobs the same way.

**Alternatives.** Serve any cached layer by digest, as the ORAS cache first
did: the digest proves the bytes. Or ask the registry for every layer
(`HEAD` on the blob in this repository) before serving it from the store.

**Trade-off.** The same weights published in two repositories are
downloaded once per repository, not once per node.

**Why (security, simplicity).** A digest proves what the bytes are, not who
may read them. With a node-wide cache keyed by digest alone, a job could
publish a manifest in its own repository that lists a private layer's
digest (from a leaked manifest, say) and get the node's copy with no
credential for the private repository. The manifest itself is always read
from the registry with the job's credential, so the registry already checks
access to the repository; requiring the layer to be verified from that same
repository closes the rest, with no extra request per layer. It is also the
image cache's rule, so there is one rule for both:

```console
$ legion job run weights.ncl   # oci://ghcr.io/attacker/w:v1, mediaType = …
staging failed: oci://ghcr.io/attacker/w:v1: pull blob sha256:…: … 404
```

## 16. The fetch defaults grow to 1 TiB and 2 hours; there is no stall timeout

**Call.** `maxArtifactSize` defaults to 1 TiB (was 50 GiB) and the fetch
deadline to 2 hours (was 10 minutes).

**Alternatives.** Keep the defaults and document raising them; add a stall
timeout (no bytes for N minutes) alongside a large deadline.

**Trade-off.** `maxArtifactSize` also bounds archive expansion, so the default
decompression bound is much weaker, and a stalled download holds its
allocation for up to 2 hours per attempt.

**Why.** A 140 GB checkpoint fails both old defaults, so "model weights work"
would have been a doc footnote. A stall timeout is the right fix and is a
TODO.

## 17. `exec` tasks get device env, not a device cgroup

**Call.** `exec` sets `LEGION_DEVICE_*` and the plugin's env, and nothing
stops the process opening other device nodes.

**Alternatives.** Attach a cgroup v2 device-control eBPF program to the
task's worker cgroup.

**Trade-off.** Two `exec` tasks on one node can use each other's GPUs if they
ignore their env.

**Why.** Device control on cgroup v2 is an eBPF program per cgroup (runc
carries its own implementation); building and loading one is its own project.
The container runtime gets real enforcement today through the OCI spec.

## 18. Node checks return a typed health report; the fingerprint stays a map

**Call.** `NodeChecker::health_check` returns `Option<NodeCheckReport { status,
message, attributes }>`; `has_fingerprinter`/`has_health_check` are gone;
`fingerprint` still returns attributes. The built-in disk check now warns
under 50% free and fails under 20% (it used to fail under 50%).

**Alternatives.** Type the fingerprint too; keep the old untyped map and
document it.

**Trade-off.** A breaking plugin change (API version 3). Nodes between 20%
and 50% free disk, unhealthy before, are healthy now.

**Why.** A fingerprint is a set of attributes, which a map already is; health
needs a verdict and prose, which a map of strings conflated (any string not
"pass" was a failure, so a message was a failure). The old disk rule marked
half-full disks unhealthy.

## 19. Node-check plugins load once; a load failure is a failing check until restart

**Call.** A node-check plugin is loaded on first use and reused for the life
of the worker; a load failure is cached and reported as a failing check
every pass.

**Alternatives.** Retry the load every pass (as the per-pass dlopen did).

**Trade-off.** Installing a fixed plugin needs a worker restart.

**Why.** Reloading a dylib every 30 seconds is the cost PLG5 named, and
re-logging the same load error every pass was noise.

## 20. `job validate` checks job rules only when it can read the policies

**Call.** `job validate` fetches namespace policies (`acl:read`) and applies
their `jobRules`; without access, or offline, it warns and says `job plan`
checks them.

**Alternatives.** A new read RPC that evaluates rules for a caller with
`job:read`; always skip in validate.

**Trade-off.** Most job authors lack `acl:read`, so their `job validate`
warns instead of checking.

**Why.** `job plan` already runs every policy check on the manager, as the
caller; a second RPC for the same answer is surface without a new job.

## 21. Sandboxes refuse devices at submit; MIG, MPS, and DRA are deferred

**Call.** A `sandbox` task with `resources.devices` is refused at submit.
MIG partitions, MPS, time slicing, and DRA-style shared claims are not
modeled.

**Alternatives.** VFIO passthrough for cloud-hypervisor; fractional `count`.

**Trade-off.** No GPU in a microVM; no sharing a GPU between tasks (a plugin
can report MIG instances as their own group, assigned whole).

**Why.** Each is a project with hardware-specific failure modes, and none can
be tested without the hardware.

## 22. Task runtime dylibs are not contained; WASM and out-of-process plugins are deferred

**Call.** Only the four small plugin traits got containment. The `TaskPlugin`
trait (about 40 methods) did not; moving in-manager hooks to WASM and task
runtimes and fetchers out of process is a TODO.

**Alternatives.** A 40-method `Contained` adapter for `TaskPlugin`.

**Trade-off.** A panicking dylib task runtime still kills its worker.

**Why.** The only dylib task runtime is `noop`, the review's direction for
task runtimes is out of process, and a 40-method forwarding adapter is a
standing trap (a method forgotten falls back to the trait default and
silently changes the plugin's behavior).

## 23. P2P distribution, lazy loading, and resumable fetches are deferred

**Call.** None of Dragonfly/Spegel-style peer sharing, SOCI/Nydus-style lazy
loading, or HTTP range resume shipped.

**Alternatives.** Serve a node's content store to peers over the worker plane
(the content index already knows who holds what).

**Trade-off.** A hundred nodes starting one model each download it from the
registry.

**Why.** Peer serving needs authentication between workers, a fetch path
that tries peers before origin, and eviction coordination: a plan of its own.
Lazy loading needs a FUSE or snapshotter layer. Both are recorded as TODOs.

## 24. Runtime classes take no extra flags; a wrapper script carries them

**Call.** A class is a `path`, with no `args`.

**Alternatives.** An `args` list inserted before each subcommand.

**Trade-off.** `runsc --platform=systrap` needs a one-line wrapper script.

**Why.** Every runtime invocation (run, exec, kill, delete, state, version)
would need the args threaded through; a wrapper is one file the operator
already understands.

## 25. Job mutation, a secrets provider, rollout hooks, and controller-side volumes are deferred

**Call.** PLG4's mutation policies and secrets provider, and PLG5's rollout
step/analysis/abort hooks and controller-side volume operations, did not ship.

**Alternatives.** CEL-based mutation (Kubernetes `MutatingAdmissionPolicy`);
a `secretRef` resolver plugin; a `step` hook on `RolloutStrategyPlugin`.

**Trade-off.** Operators can refuse jobs but not fix them up; secrets still
come from KV and workload identity; rollouts cannot pause on analysis.

**Why.** Each changes a core flow (the stored spec, every secret read, the
rollout state machine) and deserves its own plan; validation was the part a
reviewer asked for first and the part that is safe.
