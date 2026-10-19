# Host Convergence Plan (the Chef replacement)

> **Judgement calls:** [../calls/host-convergence-plan_calls.md](../calls/host-convergence-plan_calls.md)

**Status: PLANNED.** No code. This plan describes how Legion would take over the
job people currently give Chef, Puppet, Ansible, or Salt: declaring what a *host*
should look like and keeping it that way. It depends on
[host-process-supervision-plan.md](host-process-supervision-plan.md) phase 1
(real exit status) and on node-configuration jobs, which are specified here.

Gap analysis that produced it:
[../systemd-and-chef-analysis.md](../systemd-and-chef-analysis.md) §6.

## 1. Problem statement

Legion can put *files and processes* on a node. It cannot put a *node* into a
state.

Everything host-shaped has to be written as `command: ["bash", "-c", …]` inside
an `exec` task, which means:

- **No idempotence.** The author has to write "install it if it is not already
  installed" by hand, every time, for every package manager.
- **No change reporting.** A converge script's output is a log blob. Nothing can
  answer "which of my 400 nodes actually changed last night, and what changed".
- **No drift detection.** If someone edits `/etc/app/app.conf` by hand, Legion
  never notices; the task is still "running".
- **Wrong lifecycle.** A Legion task owns its mutations: stopping the allocation
  tears down its alloc dir and volumes. Host configuration is the opposite — you
  converge and leave, and the change must outlive the task that made it.
- **No run-on-join.** A node that joins the cluster gets nothing until an
  operator submits something with a constraint that happens to match it.
  `count: {num: 1, per: "id"}` approximates a daemonset but does not converge
  newly joined nodes, and `sysbatch` does not exist
  ([TODO.md](../TODO.md), "Job types").

So today a Legion cluster needs a second product to answer "what should this box
have on it", with its own agent, its own credentials, its own inventory, and its
own idea of what a node is.

## 2. Prior art

**Chef.** Resources and providers (`package`, `file`, `template`, `directory`,
`user`, `group`, `service`, `mount`, `cron`, `execute`), each idempotent and each
reporting *changed* vs *up to date*. Cookbooks and recipes in a Ruby DSL,
attributes with a precedence ladder, roles and environments, ohai facts,
`chef-client` runs on an interval, `--why-run` for a dry run, `notifies`/
`subscribes` for intra-run ordering, data bags for shared data, `knife bootstrap`
to adopt a bare host over SSH. What we take: the resource model, idempotence as
the contract, per-resource change reporting, and why-run. What we reject: the
Ruby DSL (Legion has Jsonnet), the attribute-precedence ladder (Jsonnet + KV
covers it with far less magic), and SSH bootstrap (Legion is agent-based by
construction).

**Ansible.** Agentless, push-based, YAML modules with the same idempotence
contract, `--check` and `--diff`. Its lesson is the *module boundary*: a stable
"here is desired state, tell me what you changed" interface is what makes a
thousand community modules possible. We take that boundary and put it behind
Legion's existing plugin system.

**Puppet.** A real resource graph with autorequire edges, and a catalog compiled
server-side then applied locally — very close to what Legion already does with
jobspec rendering at submit. Its lesson (and its reputation) is that a
full-blown dependency-graph DSL is where config management goes to die; ordered
lists plus explicit notifies get you most of the value.

**NixOS / image-based.** The strongest competitor to convergence is not
converging: build an image, ship it, reboot. Legion should not pretend otherwise
— this plan is for the mutable-fleet case (bare metal, long-lived VMs, edge
devices, dev boxes), and the docs should say so plainly.

**Nomad `sysbatch`** and **K8s DaemonSet**: run once on every node / one pod per
node, converging as nodes join. Both are the placement half of what is needed
here, and Legion has neither yet.

**cloud-init**: the "runs once when the machine appears" slot. Node
configuration jobs are the cluster-managed version of that, with the advantage
that the manager knows when a node joined.

## 3. Jobs to be done

**Cluster operator**
- When a new machine joins the cluster, I want it to configure itself — packages,
  users, mounts, kernel settings — before anything gets scheduled onto it, so I
  never have a half-built node running production work.
- When I change the base configuration, I want to know which nodes changed and
  which were already correct, so a fleet-wide change is reviewable instead of a
  wall of logs.
- When someone edits a config file by hand at 3am, I want to be told the node has
  drifted, so the fix is not "reboot it and hope".

**Job author / app developer**
- When my service needs a system package or a directory that must exist before it
  starts, I want to declare that next to the job, so I do not file a ticket
  against the platform team for `mkdir`.

**Security / platform admin**
- When a team can configure hosts, I want that to be a separate, explicitly
  granted power scoped to a node pool, so "can deploy an app" never silently
  means "can install a root-owned package on every box".
- When a converge run wants to install software, I want the same artifact
  provenance controls (checksums, pinned digests, credential scoping) the rest of
  Legion already enforces.

**On-call responder**
- When a host misbehaves, I want to see the last converge run for that node and
  what it changed, so I can tell a config change from an app bug.

**Autonomous agent**
- When I am asked to bring a fleet to a state, I want a dry-run that returns
  structured per-resource diffs, so I can check my work before mutating anything.

### User scenarios

1. An operator writes one `base` job. Every node that joins runs it before it is
   schedulable; `legion node converge status` shows 398 nodes up-to-date, 2
   changed, 0 failed.
2. A drift check on an interval finds `/etc/app/app.conf` modified by hand,
   reports `changed` on the next run, and emits a `converge.drift` event that
   pages nobody but shows up in the daily digest.
3. An agent runs `legion job plan` on a converge job and gets, per node and per
   resource, `unchanged | would-change | error` with a diff — and only then
   submits.

## 4. Goals and non-goals

**Goals**
- A `converge` task runtime: a list of typed, idempotent host resources with
  per-resource `unchanged | changed | failed` reporting.
- The 80/20 resource set: `package`, `file`, `template`, `directory`, `user`,
  `group`, `service`, `mount`, `line_in_file`, `command` (with `creates`/`unless`
  guards so even the escape hatch can be idempotent).
- Node configuration jobs: run on join, run on a schedule, gate node readiness,
  and publish dynamic node attributes from what they discovered.
- Dry run (`why-run`) as a first-class path, structured for agents.
- Drift reporting into the store, the event stream, and the UI.
- A host-mutation ACL boundary, scoped by node pool, plus a per-worker opt-in.

**Non-goals**
- Compliance profiles / InSpec-style auditing (a later, separable product).
- An attribute-precedence system beyond Jsonnet imports + KV.
- SSH bootstrap of unmanaged hosts. Legion needs its agent; `legion join` is the
  adoption story.
- Windows resource providers in v1 (the plugin boundary allows them later).
- Arbitrary community-module compatibility (no Chef/Ansible module shim).
- Replacing image-based provisioning where that already works.

## 5. Background — what exists today

- **Delivery is built.** `artifacts` (http/file/git/S3/OCI, checksums, digest
  pinning, KV-backed auth — `docs/artifacts.md`); `exec`'s `config.files[]`
  writes host files with modes before start (`src/task_plugins/exec.rs:349`); KV
  `env`/`files` render server-side from templates with `watch`/`onChange`/
  `onDelete` policies (`docs/kv-store.md`).
- **Facts exist.** `fingerprint_node` + node-check plugins produce fingerprint
  attrs; `config_attrs` and `update_node_dynamic` carry operator- and
  runtime-supplied attributes (`src/worker/node_check.rs`,
  `docs/jobs-tasks-and-scheduling.md`).
- **Scheduling primitives exist.** `count: {num, per}` groups nodes by attribute
  (`per: "id"` is per-node); constraints/permits/affinities filter and score;
  `lifecycle: "batch"` + `schedule` gives interval runs.
- **Plugins are the extension mechanism.** Task runtimes are dylibs loaded by
  name (`src/task_plugins/mod.rs:770`), with a declared capability surface
  (`supports_artifacts`, `exec_support`, volume modes, …).
- **Dry runs exist.** `legion job plan` / `job validate` / `job schema`, all
  emitting the standard `output::` envelope (`docs/agent-readiness.md`).
- **ACL vocabulary is `job|node|kv|state|operator|acl`** with scopes by job name,
  node pool, or KV path (`docs/auth-and-acls.md`) — there is no "may mutate this
  host" concept.
- **Missing entirely:** any notion of idempotence, change reporting, drift, or a
  run-on-join placement mode.

## 6. Proposed design

### 6.1 The `converge` task runtime

```jsonnet
{
  name: "base-config",
  runtime: "converge",
  lifecycle: "batch",
  schedule: "every 30m",        // the chef-client interval analogue
  count: { num: 1, per: "id" }, // every node (until nodeConfig, §6.3)
  config: {
    // Fail the whole run on the first failed resource (default), or keep
    // going and report every failure ("continue").
    onError: "abort",
    resources: [
      { type: "package", name: "nginx", version: "1.26.*", state: "present" },
      { type: "group", name: "app", gid: 3000 },
      { type: "user", name: "app", group: "app", shell: "/usr/sbin/nologin",
        system: true, home: "/var/lib/app", manageHome: true },
      { type: "directory", path: "/etc/app", owner: "app", group: "app",
        mode: "0750" },
      { type: "template", path: "/etc/app/app.conf", owner: "app", mode: "0640",
        template: |||
          tenant={{ kv "tenant/id" }}
          workers={{ kv "app/workers" }}
        |||,
        notifies: ["service:app"] },
      { type: "mount", path: "/data", device: "/dev/nvme1n1", fstype: "ext4",
        options: ["noatime"], persist: true },
      { type: "service", name: "app", state: "running", enabled: true },
      { type: "command", name: "sysctl-reload", command: ["sysctl", "--system"],
        unless: ["sh", "-c", "sysctl -n vm.swappiness | grep -qx 10"] },
    ],
  },
}
```

Contract for every resource type, and the whole point of the design:

1. **Inspect** current state. 2. **Decide** whether it matches. 3. **Act** only
if it does not. 4. **Report** `unchanged | changed | failed`, with a
human-readable diff for file-shaped resources.

`notifies` is a flat, explicit edge — `"service:app"` means "if I changed, act on
that resource at the end of the run". No graph solver, no autorequire: resources
run in declaration order, notifications fire once, at the end, in declaration
order. That is Puppet's value without Puppet's failure mode.

Templates reuse the existing KV template renderer, so `{{ kv "…" }}` means the
same thing here as in `files[]`, including watch/change policies.

### 6.2 Providers and the platform boundary

Resource *types* are stable and cross-platform; *providers* are per-platform and
selected from the node's fingerprint (`os`, `distro`, `pkg_manager`):

| Type | Linux providers | macOS |
| --- | --- | --- |
| `package` | apt, dnf/yum, apk, pacman | homebrew |
| `service` | systemd, openrc | launchd |
| `user`/`group` | useradd/groupadd | dscl |
| `mount` | mount + fstab | mount |
| file-shaped | native | native |

Providers live **inside** the `converge` runtime rather than being separate
plugins: they share the inspect/decide/act/report machinery, and a plugin
boundary per package manager would multiply the surface for no gain. A
third-party runtime can still implement its own resource vocabulary — the plugin
boundary that already exists is the extension point.

### 6.3 Node configuration jobs

A new job-level block, and the placement mode `sysbatch` implies:

```jsonnet
{
  name: "node-base",
  nodeConfig: {
    // Run on every node that matches, including nodes that join later.
    onJoin: true,
    // Block scheduling of ordinary work until the first successful run.
    gateReadiness: true,
    // Publish attributes discovered by the run; usable in constraints.
    publishAttrs: ["kernel_tuned", "gpu_driver_version"],
  },
  tasks: [{ name: "base", runtime: "converge", lifecycle: "batch", … }],
}
```

- `onJoin` makes the `node_join` eval path place this job's tasks on the new node
  (the eval already exists and already reconciles count-per work).
- `gateReadiness` sets node availability to a new `configuring` state until the
  first run reports success — the cordon/drain axis already models exactly this
  and the scheduler already filters on it.
- `publishAttrs` writes into the node's dynamic attrs via the existing
  `update_node_dynamic` path, so a converge run can make a node eligible for
  workloads that constrain on what it just installed.

This block is what makes `sysbatch`/system jobs real in Legion — the open TODO
items are subsumed by it.

### 6.4 Reporting, dry run, and drift

- **Per-resource results** are persisted per allocation: type, name, status,
  diff (truncated), duration. Queryable via `legion state query`, shown in the
  UI's allocation view.
- **Events**: `converge.started`, `converge.changed` (with counts),
  `converge.unchanged`, `converge.failed`, `converge.drift`.
- **Dry run**: `legion job plan` on a converge job dispatches a **why-run** to the
  targeted nodes and returns per-node, per-resource `unchanged | would-change |
  error` with diffs, in the standard JSON envelope. No mutation, ACL-gated as a
  read, and the primary agent surface.
- **Drift** is just a scheduled run whose result is `changed` when nothing in the
  spec changed. The report says which resources drifted; the event stream is what
  a digest or an alert consumes.
- **CLI**: `legion node converge <node>` (force a run), `legion node converge
  status`, `legion job plan --converge` for the fleet-wide dry run. MCP:
  `legion_converge_plan` (read-only, always listed) and `legion_node_converge`
  (`mutates: true`, gated behind `--allow-write`).

### 6.5 Security boundary

Host mutation is a genuinely new power and gets its own vocabulary:

- New ACL resource `host` with actions `read`/`write`, scoped by **node pool**.
  Submitting a job containing a `converge` task requires `host:write` on every
  pool it can land on; the why-run path requires `host:read`.
- Per-worker opt-in: `hostConverge: { enabled: true, allowedResourceTypes: [...] }`
  in the worker config. Default **off** — a worker that has not opted in refuses
  converge allocations with a typed error, so enabling the feature cluster-wide
  is a deliberate act per node.
- Converge runs execute as root by construction. They are therefore excluded
  from `exec`-style user dropping, and every run is audited: who submitted, which
  node, what changed.
- Artifacts used by converge resources go through the existing artifact
  admission (checksums, digest pinning, KV-scoped credentials).

## 7. Proto, struct, config, and persistence changes

- `proto/legion.proto`: `ConvergeResourceResult` and `ConvergeRunReport`
  (allocation id, node, per-resource results); a `converge_reports` field on
  `HeartbeatRequest`; a `PlanConverge` RPC for why-run; `nodeConfig` fields on the
  job message.
- `src/structs/job.rs`: `JobConfig.node_config`, and the `converge` task config
  schema (typed per resource kind, so `job validate` and `job schema` cover it).
- `src/structs/config.rs`: worker `hostConverge { enabled, allowedResourceTypes }`.
- New `src/task_plugins/converge/` (runtime, resource types, providers).
- Storage: `converge_result` table keyed by allocation + resource; node
  `availability` gains `configuring`.
- ACL: `AclResource::Host`, pool-scoped, plus `tests/integration/acl_endpoints.rs` entries for
  the new RPC.
- `configs/jobs/maximal.jsonnet`: `nodeConfig` block and a `converge` task.

## 8. Implementation phases

**Phase 1 — the runtime, file-shaped resources only.** `converge` runtime,
inspect/decide/act/report machinery, `file`, `template`, `directory`,
`line_in_file`, `command` with guards. Per-resource results in logs and in the
allocation record. Ships value on its own: idempotent config-file management with
real change reporting.

**Phase 2 — system resources.** `package`, `service`, `user`, `group`, `mount`,
with providers selected by fingerprint. This is the phase that lets a Chef
cookbook be retired.

**Phase 3 — node configuration jobs.** `nodeConfig` with `onJoin`,
`gateReadiness`, `publishAttrs`; the `configuring` availability state. Subsumes
the sysbatch/system-job todos.

**Phase 4 — why-run, drift, and the agent surface.** `PlanConverge`, `job plan`
integration, structured diffs, drift events, MCP tools, UI panel.

**Phase 5 — the security boundary.** `host` ACL resource, worker opt-in,
audit trail. *Ordering note:* the worker opt-in (default off) ships in phase 1 so
nothing is enabled by accident; the full ACL resource lands here.

## 9. Testing plan

- **Idempotence is the test suite.** Every resource type gets a table test:
  apply to a clean fixture → `changed`; apply again → `unchanged`; mutate the
  fixture → `changed`. Run against a temp root for file-shaped resources.
- Provider selection from fingerprints, unit-tested without touching a package
  manager.
- System-resource providers behind a Linux container integration test
  (`internal_docs/development.md` already documents Linux worker containers);
  CI runs the file-shaped set everywhere.
- Why-run parity: for every resource type, why-run's verdict must match what a
  real run then does — a property test with the fixture from the idempotence
  suite.
- ACL: `tests/integration/acl_endpoints.rs` entry for `PlanConverge`; a test that a converge
  job is rejected without `host:write`, and on a worker that has not opted in.
- Failure semantics: `onError: abort` stops at the first failure and reports the
  rest as `skipped`; `continue` reports all.

## 10. Rollout and backward compatibility

- Everything here is additive: no existing jobspec changes meaning.
- Workers must opt in (`hostConverge.enabled`), so an upgraded cluster gains
  nothing until an operator says so.
- `nodeConfig.gateReadiness` changes when nodes become schedulable, so it is
  per-job and off by default; the docs spell out the "no node config job = no
  gate" behavior.
- The `converge` runtime is a dylib like every other task runtime, so a cluster
  can simply not ship it.

## 11. Open questions

- **Where do resources live — task config or a new top-level block?** Task config
  keeps the plugin boundary honest and gets validation for free. *Lean: task
  config*, as shown.
- **Should file-shaped resources reuse `exec`'s `files[]` code path?** *Lean:
  share the renderer, not the writer* — converge needs inspect-then-write, which
  `files[]` deliberately does not do.
- **Does drift need its own scheduler, or is a scheduled batch run enough?**
  *Lean: scheduled run*, with a `driftOnly: true` flag that makes it report
  without acting.
- **Do we need `reboot` as a resource type?** *Lean: not in v1* — a reboot
  interacts with allocation lifecycle, drains, and stateful fencing in ways that
  deserve their own plan.
- **How do converge runs interact with rollouts?** A node-config job is not a
  service; `rollout` semantics probably do not apply, but a fleet-wide converge
  change is exactly the thing you want staged. *Lean: reuse `percentSteps` for
  node-config jobs in phase 3.*

## 12. Acceptance criteria

- **Phase 1**: a job declaring a templated file converges a clean node
  (`changed`), reports `unchanged` on the next run, and reports `changed` after
  the file is edited by hand — visible in `legion job info` and the event stream.
- **Phase 2**: a node with no nginx converges to nginx present, running, and
  enabled, on both an apt and a dnf host, with `unchanged` on re-run.
- **Phase 3**: a newly joined node runs the node-config job before any ordinary
  workload is placed on it, and publishes the declared attributes.
- **Phase 4**: `legion job plan -j` on a converge job returns per-node,
  per-resource verdicts and diffs, and mutates nothing.
- **Phase 5**: a token without `host:write` cannot submit a converge job; a
  worker without `hostConverge.enabled` refuses the allocation with a typed
  error.
