# Per-node replicas for apps, and the ingress on every node it is placed on

> **Judgement calls:** [app-per-node-replicas-plan_calls.md](../calls/app-per-node-replicas-plan_calls.md)

**Status: IN EXECUTION** (written 2026-11-08). Built in the same change as
this plan, on the ingress PR (#65).

Deliberately deferred:

- an eligible-node count computed ahead of placement (status counts what
  the scheduler placed; calls entry 3)
- a cap on per-node replicas (calls entry 4)
- per-node for apps whose work is not node-local, beyond what the manifest
  author chooses

---

## 1. Problem

An app's `replicas` is a fixed number, 1 to 32, rendered into every task's
`count`. That suits an app whose replicas share work (the autoscaler's
shards). It does not suit one whose replicas each serve the node they run
on, and the ingress is one: it binds ports 80 and 443, so every node that
runs it is a front door, and DNS or a load balancer points at those nodes.
With a fixed count:

- the operator has to pick a number and keep it in step with the nodes: a
  third manager or a fifth edge node gets no ingress until someone runs
  `legion app set --replicas`;
- the default, one replica, is a single point of failure on a cluster that
  has three managers, and an upgrade leaves a gap on it;
- "how many?" is a question with only one right answer for this kind of
  app: one on each node.

The scheduler already has the answer. `count = { num = 1, per = "id" }`
places one allocation on every eligible node and follows nodes as they
join and leave; the log shippers and `legion cert http01` run that way. The
apps framework just never exposed it.

## 2. Prior art

- **Kubernetes DaemonSet.** One pod per node matching a selector; a node
  joining gets one. ingress-nginx and Traefik ship both a Deployment (a
  replica count, behind a `LoadBalancer` Service) and a DaemonSet (with
  `hostNetwork` or `hostPort`) mode; the DaemonSet is the bare-metal and
  edge-pool answer.
- **Nomad system jobs.** `type = "system"` runs one allocation on every
  eligible client; Fabio and Traefik on Nomad are usually system jobs,
  constrained to a node class for the edge.
- **Consul.** Agents run on every node by design; the API gateway and
  ingress gateways are counted deployments.
- **Legion jobs.** `count = { num = 1, per = "id" }`, the same daemonset
  shape, with the capacity pass placing on new nodes.

## 3. Jobs to be done

- **Operator running the ingress on managers.** "I turned it on; it should
  survive a manager going away and an upgrade without me choosing a
  number."
- **Operator with an edge pool.** "I add an edge node; it should start
  serving. I take one away; nothing should be left pending."
- **App author.** "My app is node-local (a proxy, a node agent). I want to
  say so in the manifest, once."
- **Agent.** "`legion app status -j` should say whether every copy runs,
  whatever the mode, and `legion app set --replicas` should take the same
  words a person uses."

## 4. Goals and non-goals

Goals:

- `replicas` takes a number or `"per-node"`, everywhere it is written: the
  manifest, `legion app set --replicas`, and the manager config's seed.
- Per-node renders `count = { num = 1, per = "id" }` for every task: one on
  each node the app's placement and pool allow.
- Status and health work in both modes.
- The ingress defaults to per-node: every manager, or every node of its
  edge pool.
- The seed config takes `pool`, so a new cluster can put the ingress on an
  edge pool from the start.

Non-goals:

- Making the ingress run on every node of the cluster by default (calls
  entry 1).
- Per-node for any other bundled app: the autoscaler, cert manager, and
  GitOps share work through shards or must run once.
- Computing eligible nodes outside the scheduler.

## 5. What exists today

- `AppManifest.replicas: u32` and `AppSettings.replicas: u32`, validated
  1-32 (`crates/common/src/structs/app.rs`), rendered as `count` in `render_app_job`.
- `AppJobState::healthy` and `derive_status` (`crates/manager/src/manager/apps.rs`)
  compare `allocations.running` with `replicas × tasks`.
- `UpdateAppSettings` takes `optional uint32 replicas`; `AppSettingsInfo`
  reports `uint32 replicas`.
- `AppSeedConfig { enabled, placement, replicas, config }`.
- `legion app set --replicas N`.

## 6. Design

### 6.1 The surface

```nickel
# A manifest
replicas = "per-node",
```

```console
$ legion app set ingress --replicas per-node     # one on every node it may run on
$ legion app set ingress --replicas 2            # back to a fixed number
$ legion app status ingress
  Settings:   managers, one per node
```

```nickel
# configs/manager.ncl: seed the ingress onto an edge pool, one per node
apps = { ingress = { enabled = true, placement = "workers", pool = "edge" } },
```

Errors:

```text
Error: replicas must be 1 to 32, or per-node; to stop an app, disable it
Error: replicas: "two" is not a number or `per-node`
```

### 6.2 The API

No reshaping of existing fields:

- `AppSettingsInfo.per_node` (new): true when the app runs one per node;
  `replicas` is 0 then.
- `UpdateAppSettingsRequest.per_node` (new, optional): `true` switches to
  per-node. `replicas` switches to a fixed count. Both at once, or
  `per_node = false` without `replicas`, is `InvalidArgument`.

### 6.3 Inside

- `Replicas { Count(u32), PerNode }`, serialized as a number or the string
  `"per-node"`, so existing records and manifests read unchanged.
- Render: `PerNode` → `count = { num = 1, per = "id" }` on every task.
- Status: for a fixed count, desired is `replicas × tasks` as today. For
  per-node, desired is the latest version's live allocations (at least 1):
  what the scheduler placed. Healthy means every one of them runs healthy.
- No 32 cap for per-node: the pool bounds it.

### 6.4 The ingress

Its manifest says `replicas = "per-node"`. On managers that is one per
manager; with `--placement workers --pool edge`, one per edge node. Docs
lead with an edge pool and warn that per-node on a shared pool puts the
ingress, its token, and its TLS keys on every tenant node and takes ports
80 and 443 from tenant jobs.

## 7. Phases

One phase: the framework, the API, the CLI, the seed `pool`, the ingress
default, docs, and tests.

## 8. Testing

- Unit: `Replicas` parse and serialize; manifest and settings validation;
  render emits `{ num = 1, per = "id" }`; status in per-node mode (all
  placed healthy, one pending, none placed); `describe_change`; the seed
  applies `pool` and `per-node`; the CLI parses `--replicas per-node`.
- Integration: an app set to per-node places one allocation on each of
  several eligible manager nodes and reports `running` with desired equal
  to the nodes; `UpdateAppSettings` with `per_node` and the error cases;
  the bundled ingress seeds per-node.

## 9. Rollout

Legion has no installed base to protect. Existing records keep their
number: a stored `replicas` is still a number, and adopting a new bundled
manifest keeps the record's settings. The ingress has not shipped before
this change, so every cluster seeds it fresh, with per-node.
