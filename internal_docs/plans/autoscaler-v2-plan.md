# Autoscaler Plan (v2)

> **Judgement calls:** [autoscaler-v2-plan_calls.md](../calls/autoscaler-v2-plan_calls.md)

**Status: PLANNED** (written 2026-10-19). This plan **supersedes**
[autoscaler-plan.md](autoscaler-plan.md). That plan's Phase 1 (`legion job
scale`, count overrides, the `scale` ACL resource) shipped on 2026-07-30 and
is still accurate. This plan treats it as **Phase 0** and builds on it
unchanged. Everything after that phase is redesigned here. The old calls doc
([autoscaler-plan_calls.md](../calls/autoscaler-plan_calls.md)) still governs
the shipped surface. [The ledger in the new calls doc](../calls/autoscaler-v2-plan_calls.md#ledger-what-stands-from-the-superseded-plans-calls)
lists which of its entries this plan keeps and which it reverses.

The autoscaler is the first **Legion app**. It runs on the substrate
described in the companion plan, [legion-apps-plan.md](legion-apps-plan.md):
jobs in the reserved `legion-system` namespace, hosted by default on a
namespace-restricted worker inside each manager, with permissions the app
declares, and UI panels it registers. GitOps is meant to be the second
consumer of that substrate. This plan covers only what is specific to
scaling.

Deliberately deferred, and named in §4: waking a request-serving `service`
from zero (blocked on the gateway routing to services, not only keyed
instances), autoscaling keyed instances and `count.per` tasks, vertical
(request-size) scaling, predictive scaling, and WASM strategies.

---

## 1. Problem statement

Phase 0 made *manual* scaling cheap. `legion job scale web --count 6` writes
a count override. It creates no new version and starts no rollout. But
nothing decides that count for you, and nothing grows or shrinks the
machines underneath it.

1. **No automatic task scaling.** A service at 95% CPU stays at its count
   until someone runs `job scale`. A queue worker with a backlog of a
   million messages runs the same three replicas as one with an empty
   queue.
2. **No cluster scaling.** Nodes appear only when someone runs `legion join`
   somewhere. If a scale-out cannot place, the job reports `blocked` and
   waits (`docs/scheduler-and-eval-loop.md` § Blocked evals). Nothing
   provisions a node. Idle nodes also run forever: there is no node-removal
   RPC (`Store::remove_node`, `src/storage/store.rs:7940`, has no caller
   outside tests), and lost nodes are never garbage-collected.
3. **The inputs exist, but only in pieces.** Per-allocation CPU and memory
   usage now reaches the manager on every heartbeat (`UsageReport`,
   `proto/legion.proto:6611`). It stays in memory on *whichever manager
   received the heartbeat* (`src/manager/usage.rs:1-25`; "another manager
   answers with `usage_reported: false`", `docs/resource-enforcement.md`
   § What usage does not do). Per-service request counts exist too
   (`NetEdgeSample`, folded into 15 minutes of per-minute buckets, served by
   `ListServiceEdges`), with the same per-manager split. No single place can
   answer "how busy is `web`, cluster-wide, right now?"
4. **Nothing to extend.** A team with its own metrics (a Kafka lag gauge in
   Prometheus, a cost model, a Postgres connection count) or its own
   machines (an ASG, a Hetzner project, a rack of bare metal behind
   Terraform) has no seam to plug into.

What users want here is narrow: *set `autoscale = true` on a task and trust
it*. Add a few lines for safeguards. Add a plugin only if your signal or
your machines are unusual. Read one line that says why anything changed.

## 2. Prior art

We take something from each of these systems and avoid at least one mistake
from each.

### Nomad Autoscaler ([docs](https://developer.hashicorp.com/nomad/tools/autoscaling), [repo](https://github.com/hashicorp/nomad-autoscaler))

- **Three plugin kinds**: APM (metrics), strategy (count), target (what to
  scale). Plugins are separate binaries using HashiCorp's go-plugin, which
  is gRPC over a local socket. *Take:* the three-way split, verbatim. It is
  what the user asked for, and it is the cleanest part of Nomad's design.
- A separate daemon, usually deployed as a job. Policies live in two places:
  jobspec `scaling` blocks for task groups, and HCL files on the agent's disk
  for cluster policies, which are not in cluster state. It is
  single-instance until HA arrived in v0.4. *Take:* running it as a job.
  *Reject:* policies on the daemon's disk, and wiring its token by hand.
- **Scaling creates a job version, and resubmit resets the count**
  ([nomad#9839](https://github.com/hashicorp/nomad/issues/9839)). Phase 0
  already fixed this for Legion with count overrides and `base_count`
  arbitration.
- **Cluster scale-in.** The target plugin picks victims (`least_busy`,
  `empty`), drains them with a deadline, and terminates exact instances.
  *Take*, with the drain owned by Legion's core rather than by each plugin.

### Kubernetes HPA ([docs](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/))

- **The algorithm:** `desired = ceil(current × metric / target)`, with a 10%
  tolerance band. Several metrics combine as the max of their desired counts.
  *Take* all of it.
- **`behavior` policies.** Per direction: a `stabilizationWindowSeconds`
  (up 0s, down 300s; scale-down takes the max recommendation over the
  window), plus rate policies ("at most 4 pods or 100% per 15s", and
  `selectPolicy: Max|Min|Disabled`). This is the best-proven safeguard
  surface anywhere. *Take* the model, with fewer knobs and friendlier names.
- **Unready pods and missing metrics.** For scale-up, a pod that is not
  ready counts as 0% utilization. For scale-down, it counts as 100%. When a
  metric is missing, the HPA leans toward not acting. *Take.*
- **Status conditions.** `AbleToScale`, `ScalingActive`, and `ScalingLimited`
  carry a reason and a message. This is the only place most users ever learn
  why an HPA did nothing. *Take*, as the pipeline report in §6.6.
- **Manual scale gets fought.** `kubectl scale` on an HPA target is
  overwritten within seconds. *Reject:* a manual scale becomes a time-boxed
  pin instead (§6.5).

### KEDA ([docs](https://keda.sh/docs/latest/concepts/))

- **Two thresholds per trigger.** `activationThreshold` handles 0↔1 and the
  normal threshold handles 1→N. Waking from zero uses an event (queue depth,
  cron), never "the CPU of pods that don't exist". *Take.*
- **External scaler protocol.** A scaler is a gRPC service with
  `IsActive / GetMetricSpec / GetMetrics / StreamIsActive` that runs as its
  own deployment. *Take* the idea that a plugin can be a *service* as well as
  a local binary (§6.8).
- **`fallback` replicas** after N consecutive scaler failures, and a
  **pause annotation**. *Take* the pause. *Reject* fallback as a default:
  missing data holds the count instead of moving it.

### Knative KPA ([docs](https://knative.dev/docs/serving/autoscaling/))

A concurrency target per replica (default 100), a 60s stable window, and a
6s panic window that doubles capacity when load spikes. Scale-to-zero goes
through the activator, which holds requests while a replica wakes. *Take:*
"in-flight requests per replica" as a later signal once the datapath
attributes ingress traffic (§6.7). The activator is the shape Legion's
gateway already has for keyed instances.

### Cluster Autoscaler and Karpenter ([CA FAQ](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md), [Karpenter](https://karpenter.sh/docs/concepts/disruption/))

- **CA scales up on unschedulable pods.** It simulates scheduling them onto
  a template node of each node group, and picks a group with an **expander**
  (`least-waste`, `priority`, `random`). It scales down a node that has been
  under 50% utilization for 10 minutes, *and only if every pod on it could
  be placed elsewhere*. It drains before terminating, and honors
  `safe-to-evict=false`. *Take:* pending work is the scale-up signal,
  simulated against the real scheduling rules. Also the expander, and the
  scale-down rules.
- **Karpenter disruption budgets** (`budgets: [{ nodes: "10%" }]`, with
  schedules), and `consolidateAfter`. *Take:* budgets as the node scale-in
  safeguard. *Defer:* consolidation (repacking onto fewer nodes).

### Cloud autoscalers

- **GCP MIG scale-in controls**: "remove at most N VMs (or N%) per T
  minutes". This is exactly the "don't scale down too aggressively" knob,
  stated in one line. *Take*, as `down.step` and `down.every`.
- **ECS target tracking**: missing data never scales in. When several
  policies apply, any one may scale out, but all must agree to scale in.
  Scale-in is suspended during deploys. Phase 0 already adopted the deploy
  rule.
- **AWS ASG instance warmup**: new instances don't count toward the metric
  until warm. *Take*, as `warmup` (§6.5).

### Recommendation modes

The Kubernetes VPA's `updateMode: Off` computes recommendations without
acting, and many teams run it that way for weeks before they trust it.
*Take*, as `mode = "dry-run"`, available per policy and cluster-wide.

## 3. Jobs to be done

**Job author**
- When I ship a web service, I want to turn on autoscaling with one word and
  have it behave sensibly, so I don't have to learn a policy language to
  survive a traffic spike.
- When my workload is driven by a queue, not by CPU, I want to scale on the
  queue's depth from the metrics system I already have, so that replicas
  follow the actual backlog.
- When I add autoscaling, I want to see what it would have done before I let
  it act, so that I trust it before it has touched production.

**Cluster operator**
- When work can't place because the cluster is full, I want machines added
  for me from the pool I configured, so nobody gets paged to add a machine
  by hand.
- When machines sit empty overnight, I want them drained and returned, a few
  at a time and never below a floor, so the bill follows usage without risk.
- When my machines come from something Legion doesn't know about (an ASG,
  Terraform, a bare-metal API), I want to plug that in with a small program
  in any language, so I'm not waiting on Legion to support my provider.
- When I set up a cluster, I want the autoscaler to be there already, with
  the right permissions, so that turning it on is not a project.

**On-call responder**
- When a service's replica count jumped or dropped, I want one command that
  says what changed, when, and why, in plain words, so I can tell a
  legitimate spike from a broken signal.
- When autoscaling is making an incident worse, I want to freeze it for one
  service or for everything, for an hour, so it stops fighting me without
  being forgotten later.
- When autoscaling *didn't* act, I want to see which safeguard held it, so I
  know whether to raise a limit or leave it alone.

**Security / platform admin**
- When I grant teams autoscaling, I want it to resize their jobs and nothing
  else, so the autoscaler's credential is not a deploy credential.
- When a tenant writes a scaling policy, I want it to use only the metric
  sources and machine providers I installed, so a policy can't run code or
  reach URLs with the autoscaler's identity.

**Autonomous agent**
- When I manage a service, I want to read why it is at its current size and
  what the autoscaler would do next, as structured data, so I can reason
  about capacity without scraping text.
- When I change a policy, I want a dry run against recent history, so I can
  check that the change does what I intend before it acts.

**Integration author** (new persona: someone writing a plugin for their
cloud or metrics system)
- When I write a plugin, I want a tiny, documented protocol in any language
  and a test harness, so a working plugin takes an afternoon, not a Rust
  toolchain.

### User scenarios

**S1: one word.** A team adds `autoscale = true` to their `web` task, which
has `count = 2` and `resources.cpu = 500`. Legion holds average CPU near 70%
of the reservation, between 2 and 8 replicas. It scales out within a minute
of a spike, adding at most 100% or 4 replicas per minute, whichever is
larger. It scales in slowly: after five quiet minutes, it removes at most 10%
or 1 replica per minute, whichever is larger. It never scales in while a rollout is
running or while metrics are missing. `legion scaling explain web` says why
the service is at 6 right now.

**S2: safeguards.** The payments team caps `api` at 16 replicas and holds a
floor of 10 during business hours. Scale-in is limited to one replica every
two minutes. They run the policy in `dry-run` for a week and read
`legion scaling history api` to see what it would have done, then switch to
`enforce`.

**S3: queue to zero.** An ingest worker scales on
`sum(kafka_consumergroup_lag{group="ingest"})` from the team's Prometheus,
with one replica per 500 messages, sleeping at zero when the queue is empty.
The lag check's `activation = 1` is what wakes it.

**S4: the cluster grows itself.** The operator installs the `aws-asg` plugin
and applies one node-group policy. A launch-day scale-out can't place. The
autoscaler asks the manager how many nodes of that group's shape would fit
the pending work, raises the ASG's desired capacity, and the new instances
join with a pre-minted token and the group's attributes. Overnight, nodes
that have been empty for 10 minutes are drained and terminated, at most 10%
at a time, never below `min`, and removed from `legion node list`.

**S5: 3am.** `web` (bounds 6..40) went from 6 to 30. The responder runs
`legion scaling history web`. The record says CPU went to 0.96 because a
deploy doubled per-request work. They run
`legion job scale web --count 12 --for 2h --reason "bad deploy, rolling back"`,
which pins the count. The autoscaler shows `pinned until 05:10` and resumes
on its own afterwards.

**S6: a custom plugin.** A platform team writes a 60-line Python strategy
that adds replicas ahead of known batch windows from their calendar. It
speaks JSON-RPC on stdin and stdout. They test it with
`legion scaling plugin test ./calendar.py`, list it in the autoscaler's app
config, and reference it from policies by name.

## 4. Goals and non-goals

**Goals**

1. `autoscale = true` on a task gives a sensible, safe default with no other
   configuration, on any cluster with resource enforcement on (the default).
2. Safeguards are first-class and layered: bounds, per-direction
   stabilization and step limits, warmup, tolerance, missing-data holds, the
   rollout rule, pins, pauses, schedules, budgets, and dry-run. Every layer
   reports when it changed the answer.
3. Every decision can be explained: a durable record for each action, a live
   explanation for each hold, events, metrics, CLI, MCP, and UI.
4. Three plugin kinds (metric sources, strategies, targets) with built-ins
   for the common cases, and one language-agnostic protocol for the rest.
5. Node autoscaling driven by pending work and simulated against the real
   scheduling rules, with drain-before-terminate scale-in.
6. The autoscaler runs as a Legion app. It is out of the manager process,
   hosted on managers by default, can be moved to workers, and scales
   horizontally by sharding policies.
7. Least privilege by construction: the app can resize and read, not deploy;
   tenant policies can only use operator-installed plugins.

**Non-goals**

- **Waking a request-serving `service` from zero on the first request.**
  `legion start gateway` holds requests only for **keyed instances** today
  (`docs/gateway.md` § limitations: it "does not route to slot-mode
  instances or services"). `min = 0` ships, but only with an activating
  check on a signal that exists at zero replicas (a queue, a schedule, an
  external count). Service wake is a gateway feature, and this plan only
  reserves the hook (§6.12).
- **Autoscaling keyed instances and `count.per` tasks.** Both are rejected
  by Phase 0 for reasons that still hold (old calls entry 14).
- **Vertical scaling** (changing `resources`), and **predictive scaling**
  (forecasting). The strategy seam admits both later.
- **Node consolidation** (repacking running work onto fewer nodes). Scale-in
  removes empty and nearly empty nodes, and nothing more.
- **Choosing instance types** (Karpenter's flexibility). A node group has one
  shape.
- **A new raft lane.** Policy objects and scale history ride Control, where
  `task_scale` already lives (old calls entry 5).
- **WASM plugins.** There is no WASM runtime in the tree (no `wasmtime`,
  `wasmer`, or `wasmi` in `Cargo.lock`). The out-of-process protocol covers
  every plugin kind now. A pure-function strategy ABI can follow once a
  registry needs to distribute untrusted logic.

## 5. Background: what exists today

Code recon, verified against the tree on 2026-10-19.

**Phase 0 (shipped).**
- RPCs: `ScaleTask` and `GetJobScaleStatus` (`proto/legion.proto:92-107`,
  handlers `src/manager/server.rs:6021,6216`). `ScaleTaskRequest` carries
  `name`, optional `task_group`/`task_name`, `count`, `reset`, `reason`, and
  `idempotency_key`. Scale-out runs namespace quota admission
  (`server.rs:6111-6122`).
- `task_scale` table, keyed by `(job_id, task_group, task_name)`, owned by
  the Control lane (`migrations.rs:1094-1106`, `raft/ownership.rs:209`).
  `source` is hard-coded to `"operator"`; `"autoscaler"` is reserved in
  comments (`proto:3598`, `store.rs:958`).
- One accessor, `Store::effective_task_count` (`store.rs:15984`). It is
  called by the scheduler overlay, rollouts, surplus retirement, gangs, slot
  retirement, the capacity pass, and quota.
- `job_scale` eval: retire surplus, then `Reschedule` the shortfall
  (`src/structs/eval.rs:410-438`).
- Events: `scale.applied`, `scale.deferred`, and `scale.reset`
  (`src/storage/events.rs:1105-1177`).
- **Hardening found during recon:** `GetJobScaleStatus` computes
  `override_count.unwrap_or(spec)` without the `base_count` filter that
  `effective_task_count` applies (`server.rs:6250`). Today this can't be
  reached through the API, because submit arbitration deletes a mismatched
  override in the same transaction (`store.rs:10838-10850`). Pins and the
  autoscale-aware arbitration (§6.3) add new override states, so Phase 1
  routes status through `effective_task_count` as well.

**Jobspec shape.** A job is a flat list of tasks, and each task has a
`group` string (default `"default"`, `src/structs/job.rs:122`). There are no
group objects. Tasks that share a group are co-placed and all-or-nothing,
but each task keeps its own `count` (`docs/scheduler-and-eval-loop.md`
§ Partial placement). The scaling unit has therefore been the task. For a
multi-task group, this plan scales the group's tasks together (§6.3).

**Usage.**
- Workers sample cgroup v2 (`memory.current`, `memory.peak`, anon memory,
  `cpu.stat usage_usec`, PSI) every 10 seconds, at a fixed interval
  (`src/worker/usage.rs:24,264-271`). Sampling runs only when
  `resourceEnforcement.enabled`, which is the default.
- Only governed runtimes are sampled: exec, container, and sandbox. `noop`
  and third-party runtimes report nothing.
- The manager holds the latest report per node in memory, stale after 120s.
  It is never written to the store, and it is per receiving manager
  (`src/manager/usage.rs`). CPU arrives as `cpu_millicores` and is converted
  with the node's `mhz_per_core` (`usage.rs:317-321`).

**Requests.**
- `NetEdgeSample` (`proto:6736-6769`) carries `requests`, `errors`,
  `latency_ms_sum/max`, and bytes, per (calling alloc, service, tag). It is
  folded into per-minute buckets for 15 minutes, in memory
  (`server.rs:8339`).
- Edges are recorded on the **caller** side, so traffic from outside Legion
  into an advertised port is not an edge (`docs/network-handlers.md:1583`).

**Liveness digest.** Every manager that does not lead the status lane sends
the leader one RPC per second with the node ids it heard from
(`docs/node-liveness.md` § Managers that do not own liveness). The cost is
"managers × 1 RPC/interval, regardless of fleet size." This is the transport
Phase 1 uses to roll usage up.

**Pending work.**
- The planner reports unplaced replicas in memory
  (`PlanOutcome.unplaced`, `src/manager/scheduler.rs:369`).
- Blocked evals exist only when *nothing* placed. They store one key and one
  per-placement shortfall (`src/manager/blocked.rs:43-84`), with keys
  `class:pool=…`, `node:<id>`, and `escaped`.
- No RPC lists unplaced demand. The capacity pass finds jobs short of their
  count with SQL (`job_configs_waiting_on_capacity`, `store.rs:16368`). That
  is the read Phase 5 builds on.

**Placement rules a simulation must reuse.**
- Pools act as taints, and permits act as tolerations: `PermitSet` adds an
  implicit `pool=default`, and `is_permitted` requires every node
  restriction to be matched (`src/constraints.rs:114-175`).
- The rest: constraints, `distinct`, static ports, oversubscription ratios
  (`src/resources.rs:1421`), gang quorum (`GangReport`, `proto:3330`), and
  session envelopes that sit one per node via `per(1, "id")`
  (`src/structs/session.rs:410`).

**Nodes.**
- Drain: `SetNodeAvailability` (`proto:1511`) evicts stateless allocations
  immediately and protects stateful ones through `onLost`. It has no
  deadline and no "drained" signal.
- Join tokens: `CreateJoinToken {worker_type, ttl, description}` are
  **multi-use**, default 24h (`server.rs:11780-11803`).
- There is no per-host attribute at join. Per-type `attributes` come from
  worker-type config, and `legion join` takes `--name` but no `--attr`
  (`src/cmd/join.rs`).
- No removal RPC, and no GC for lost nodes.

**Background loops.** `start_node_status_checker` (`src/manager/daemon.rs:1558`)
and the lane gate `Store::is_raft_leader` (`store.rs:2247`) are the pattern
the old plan copied for an in-core loop. This plan does not use it (calls
entry 1).

**Auth.**
- The `legion` reserved workload identity authenticates a task as
  `legion:<sub>` (`src/manager/auth_method.rs:555-660`). Policies attach
  only through `workloadIdentity.controlPlaneBindings` in manager config,
  which are cloned at startup (`daemon.rs:344`).
- `LEGION_ADDR` is not injected into tasks: `configs/jobs/cert-renewer.ncl`
  hard-codes it. The apps plan fixes both.

**Plugins.**
- Five dylib kinds use `Box<dyn Trait>` across an unstable ABI
  (architecture audit M6, still open).
- Three exec-JSON seams exist: volumes, the keyring root-key provider, and
  the cluster-CA provider. The latter two go through
  `util::run_json_command` (`src/util.rs:465`).
- No gRPC or WASM plugin ABI exists.
- Legion already speaks JSON-RPC 2.0 over stdio for `legion mcp serve`.

**Events.** Best-effort, capped at 10,000 rows, "not an audit log"
(`docs/event-stream.md:262`). The durable record of a scale is the
override row.

## 6. Proposed design

### 6.1 Shape of the system

```
                ┌──────────────────────── manager (any) ─────────────────────────┐
  jobspec       │ RunJob ── validates `autoscale` ── task_config row               │
  autoscale=…  ─┤ ScalingPolicy objects (Control)   scale_history (Control)       │
  scaling apply │ ScaleTask (+expected_version, +decision) ── task_scale (Control)        │
                │ GetScalingInputs ◀── usage/edge rollups (status-lane leader)    │
                │ GetCapacityDemand ── simulate pending work on node templates    │
                │ WatchScalingPolicies (stream)        ForgetNode                 │
                └───────────────▲──────────────────────────────▲──────────────────┘
                    gRPC, app identity (scale:write, job:read, node:read/write)
                ┌───────────────┴─── legion-system/autoscaler (an app) ─────────┐
                │ policy cache ── shard leases ── evaluator ── safeguard pipeline│
                │ plugin host:  sources │ strategies │ targets                    │
                │   built-in (in process) │ external (stdio or HTTP JSON-RPC)     │
                │ HTTP: views for the UI, /metrics, decision buffer               │
                └─────────────────────────────────────────────────────────────────┘
```

The manager owns **state and the scheduling rules**: desired counts,
policies, history, usage rollups, and demand simulation. The app owns
**decisions**. Every write the app makes goes through the same public RPC a
human uses. If the app dies, scaling pauses and nothing else is affected.
The manager can tell when the app is gone and says so (§6.6).

### 6.2 One concept: a scaling policy

A **scaling policy** is a *target* (what to resize), *checks* (signals, each
with a strategy that turns the signal into a count), *bounds*, and
*behavior* (safeguards). There are two places to write one, both
compiling to the same internal `ScalingPolicy`:

| Home | Targets | Written by | Lifecycle |
| --- | --- | --- | --- |
| `autoscale` on a task in a jobspec | that task (or its group, §6.3) | job author | versioned with the job; removed with it |
| a standalone policy object, `legion scaling apply` | a node group, or an external system through a target plugin | operator (nodes), or a namespace owner (external) | its own object on the Control lane, with versions |

Standalone policies are new objects rather than manager config. Manager
config differs per manager, and node groups belong to the cluster. They are
not files on the autoscaler's disk either, which was Nomad's choice and
keeps policies out of cluster state. So they are first-class (calls entry 8):

```console
$ legion scaling apply nodes.ncl
scaling policy workers-general: created (v1)
$ legion scaling list
NAME / TARGET                      KIND   NAMESPACE      MODE     BOUNDS
web/default/web                    task   default        enforce  2..8
payments-prod/api/default/api      task   payments-prod  dry-run  4..16
workers-general                    nodes  -              enforce  1..20
```

### 6.3 `autoscale = true`, and exactly what it means

The jobspec field is `autoscale`. It takes a boolean or a record (calls
entry 17):

```nickel
{
  name = "web",
  task = {
    name = "web",
    count = 2,                       # with autoscale: the floor
    resources = { cpu = 500, memory = 512 },
    autoscale = true,
  },
}
```

`true` means "the defaults". The built-in defaults are below, and every
one of them can be overridden per task, per namespace, or cluster-wide
(next subsection). `legion job plan` prints the effective record, so nothing
is hidden:

```nickel
autoscale = {
  min = 2,                          # "1x": 1 × count
  max = 8,                          # "4x": 4 × count
  cpu = 0.7,                        # hold average CPU at 70% of the reservation
  up   = { stabilize = "0s", step = { percent = 100, replicas = 4 }, every = "1m" },
  down = { stabilize = "5m", step = { percent = 10,  replicas = 1 }, every = "1m" },
  warmup = "30s",
  tolerance = 0.1,
  window = "1m",
  mode = "enforce",
}
```

```console
$ legion job plan web.ncl
...
autoscale web/default/web:
  hold cpu at 70% of 500MHz per replica, 1m average over ready replicas
  bounds 2..8 (min = count, max = 4 × count)
  up:   immediately, at most +100% or +4 per minute (whichever is larger)
  down: after 5m below target, at most −10% or −1 per minute
```

The calls doc defends each default. In short:

- **`min = count`.** The count you wrote is the floor, so autoscaling only
  ever *adds* capacity unless you lower `min` explicitly. A three-replica HA
  service never drops to one because traffic was quiet. Editing `count` in
  the spec keeps doing what it says: it raises or lowers the floor (calls
  entry 2).
- **`max = 4 × count`.** This absorbs a 4× spike without a runaway signal
  being able to spend the cluster. Hitting it emits `scale.limited`, with a
  hint to raise `max`.
- **CPU only.** Memory is a poor default signal. It often doesn't fall when
  load falls (caches, GC heaps), and a memory check merged with max would
  then block scale-in forever. Memory is one line to opt into:
  `memory = 0.8`.
- **Utilization relative to the *reservation*.** This is what the scheduler
  packs on and what the operator pays for. A task with `cores = N` is
  measured against `N × 1000` millicores. CPU is a weight, so a task can
  burst above its reservation (`docs/resource-enforcement.md`); utilization
  above 1.0 is a valid reading and scales out.

#### Overriding the defaults

Every default can be overridden, one field at a time, at three levels. Each
level takes the same record shape as `autoscale`. A field set at one level
wins over every level below it, and a field left unset falls through
(calls entry 3).

| Level | Where | Who can set it | Typical use |
| --- | --- | --- | --- |
| task | `autoscale = { … }` in the jobspec | the job author | `autoscale = { max = 20 }` |
| namespace | `autoscale = { … }` in a namespace policy | `acl:write` (namespace policies' existing gate) | prod namespaces scale in more slowly |
| cluster | `legion scaling defaults apply defaults.ncl` | `operator:write` | every task targets 60% CPU |
| built-in | the record above | Legion | |

```nickel
# defaults.ncl  —  legion scaling defaults apply defaults.ncl    (cluster)
{ cpu = 0.6, max = "6x", down = { stabilize = "10m" } }

# payments.ncl  —  legion namespace policy apply -f payments.ncl  (namespace)
{
  name = "payments",
  namespaces = ["payments-*"],
  autoscale = { down = { step = { replicas = 1 }, every = "2m" }, mode = "dry-run" },
}

# the jobspec  (task)
autoscale = { max = 20 },
```

The rules:

- **Field by field, including nested records.** `down = { stabilize = "10m" }`
  keeps the default `down.step` and `down.every`. The one exception is a
  `step` record, which replaces the whole `step` below it, both halves
  (§6.4).
- **`min` and `max` take a number or a multiple of `count`.** Write
  `max = 20`, or `max = "6x"`. The built-ins are `min = "1x"` and
  `max = "4x"`, so a cluster can change the ratio for every task without
  knowing their counts.
- **Signals are all or nothing.** If the task's record names no signal
  (`cpu`, `memory`, or `checks`), the effective default signal applies: the
  built-in `cpu = 0.7`, or whatever the cluster or namespace set. If it
  names any, it gets exactly the ones it wrote, so
  `autoscale = { checks = [lag] }` does not also scale on CPU. Defaults may
  set `cpu` and `memory`, never `checks`, because checks name sources and
  queries that only make sense for one task.
- **Namespace policies keep their existing rule.** All matching policies
  apply. Two that set the same field to different values make that field
  unresolvable for tasks in the overlap, and the submit fails naming both
  policies, exactly as two different `pools.default` do today
  (`docs/namespaces.md` § Namespace policies). There is no precedence among
  policies to learn.
- **Defaults are resolved live, not frozen at submit.**
  - The jobspec stores what the author wrote: `true`, or the sparse record.
  - The manager recomputes the effective policy whenever any level
    changes, and `WatchScalingPolicies` delivers the recomputed policy.
  - So an operator who finds scale-in too eager everywhere changes one
    cluster default, and every inheriting task follows, with no resubmits.
  - Each `scale_history` row carries the effective policy's digest, so a
    change in behavior traces back to the defaults change that caused it.
- **A defaults change is checked against everything that inherits it.**
  `scaling defaults apply` and `namespace policy apply` recompute every
  affected policy first. If any would become invalid (a new cluster `min`
  above some task's explicit `max`, say), the apply lists them and refuses.
  With `--force`, those policies hold with status `invalid: <why>` until
  fixed. `--dry-run` prints the diff of effective policies without writing.

Where each value came from is always one command away:

```console
$ legion scaling explain payments-prod/api --policy
payments-prod/api/default/api   effective policy 9c1e…
  field            value          from
  min              4              built-in ("1x" of count 4)
  max              20             jobspec
  cpu              0.6            cluster defaults
  down.stabilize   10m            cluster defaults
  down.step        1 replica      namespace policy payments
  down.every       2m             namespace policy payments
  up.*             (defaults)     built-in
  mode             dry-run        namespace policy payments

$ legion scaling defaults show -n payments-prod    # the same table without a task
```

#### What the default can't know

What the default can't know, it says at submit:

```
warning: web/default/web: autoscale holds cpu at 70% of the reservation, but this task
  uses the default cpu reservation (100MHz). Set resources.cpu to what one replica needs.

error: batch/default/load: autoscale needs measured usage, and runtime 'noop' does not
  report it. Add checks with another source, or remove autoscale.
  hint: legion scaling explain --sources lists what each source can measure
```

**Multi-task groups.** If `autoscale` is set on a task whose group has other
tasks, the policy targets the **group**: every task in it moves to the same
count in one write, because the group is what gets co-placed. Utilization is
then the sum of the group's usage over the sum of its reservations.
Validation requires at most one task in the group to carry `autoscale`, and
all of the group's tasks to share a count. That is the honest version of
"autoscale a task group" in a jobspec that has no group objects (calls
entry 15).

**Ownership** follows Phase 0's rule 1 (old calls entry 4): with `autoscale`
present, the policy owns the live count, and resubmits never reset it.
`count` feeds `min` (unless `min` is explicit) and the starting count.

This needs a change to the shipped arbitration. Today, a resubmit whose spec
count differs from `base_count` deletes the override
(`store.rs:10838-10850`), so the live count would fall back to `count`. For
autoscaled tasks, `handle_job_submit_apply` instead keeps the override,
updates its `base_count`, and clamps it into the new `[min, max]` in the same
transaction. Raising `count` from 2 to 4 therefore lifts a task at 3 to 4
at once and leaves a task at 6 alone. Lowering `count` only lowers the
floor, and the autoscaler brings the count down at the scale-in rate. When
`autoscale` is removed in a resubmit, Phase 0's rule applies again on that
same submit.

**Editing the policy restarts nothing.** For an autoscaled task, a resubmit
whose only differences are in `autoscale` and `count` creates a job version
and moves the running allocations onto it in place. That is the path KV
re-renders already use (`src/manager/server.rs:4150-4180`). Arbitration
adjusts the live count in the same transaction. Without this rule, raising
`max` would roll the service, because `count` is in the scheduling-relevant
set (`task_schedule_equivalent`, `store.rs:21251`). Changing a cluster or
namespace default touches no job at all.

**Why `autoscale` is a core field, not an app extension.** Third-party apps
declare their own jobspec keys at extension points
([legion-apps-plan.md](legion-apps-plan.md) §6.6), and the autoscaler could
have declared `autoscale` that way (calls entry 9). It doesn't, because core behavior depends on
the field:
- submit arbitration (who owns the live count);
- the `--for` rule on manual scales;
- the in-place resubmit rule above;
- the layered defaults;
- submit errors that need the task's runtime and count.

App-extension validation is best-effort by design: an unreachable app lets
the submit through. These errors must be hard. And the bundled app is the
same binary as the manager, so the extension mechanism's main benefit,
versioning apart from Legion, buys nothing here.

### 6.4 The full policy surface

Everything beyond `true` is optional. A field you leave out takes its
effective default: the built-in value above, or whatever the cluster or
namespace overrode it to (§6.3).

```nickel
autoscale = {
  min = 4,
  max = 16,
  # Shorthand targets: CPU and/or memory as a fraction of the reservation.
  cpu = 0.65,

  # Explicit checks. Each check yields a desired count; the max wins.
  checks = [
    {
      name = "lag",
      source = "team-prom",          # an operator-installed source (§6.8)
      query = "sum(kafka_consumergroup_lag{group=\"ingest\"})",
      window = "2m",
      strategy = { name = "per-replica", value = 500 },   # ceil(lag / 500)
      activation = 1,                # wakes the task from zero (§6.12)
    },
    {
      name = "latency",
      source = "legion",
      query = "service.latency_ms.mean",  # internal callers only (§6.7)
      strategy = { name = "threshold", steps = [
        { above = 250, add = 2 },
        { above = 500, add = 4 },
      ] },
      scaleIn = false,               # this check may push up, never pull down
    },
  ],

  # Safeguards.
  up   = { stabilize = "30s", step = { replicas = 2 }, every = "1m" },
  down = { stabilize = "10m", step = { replicas = 1 }, every = "2m" },
  headroom = 1,                      # add N replicas after the merge
  warmup = "45s",
  schedule = [
    { cron = "0 8 * * 1-5", for = "10h", min = 10, timezone = "America/New_York" },
  ],
  onMissing = "hold",                # hold (default) | ignore
  mode = "dry-run",                  # enforce (default) | dry-run
}
```

- **Strategies** (built-in): `target-value` (the HPA formula, which the
  `cpu`/`memory` shorthands use), `per-replica` (`ceil(metric / value)`),
  `threshold` (step rules), `pass-through` (the metric *is* the count, so an
  external system decides), and `fixed`. Schedules are bound overrides, not
  a strategy: they raise `min` or lower `max` in a window.
- **Merge** is max of desired, across checks that have data. A check with
  `scaleIn = false` contributes only when it wants more than the current
  count. This replaces Nomad's check groups and has no veto deadlock.
- **`step`** is the larger of `percent` of the current count and
  `replicas`, per `every` window. It is a rolling window: the evaluator sums
  the changes it applied in the last `every` and allows only the remainder.
  Writing `step` replaces the whole default record, and a half you leave out
  is 0. So `step = { replicas = 2 }` means exactly 2, not "2 or 100%".
- **`onMissing`.** With `hold`, the default, nothing scales *in* while any
  check has no data. Checks that do have data may still scale out. With
  `ignore`, a check with no data is dropped for that evaluation, and the
  remaining checks decide in both directions. Use `ignore` for a check that
  is optional by nature, such as a latency guard on a service with no
  callers at night.

### 6.5 Safeguards: an explicit, self-reporting pipeline

The user asked for safeguards that don't trade away simplicity. The answer
is one fixed pipeline. Each stage either passes the number through or
changes it and records why. The records *are* the explanation, so
observability needs no separate code path (calls entry 6).

```
 checks ──▶ recommendations ──▶ merge (max) ──▶ headroom ──▶ stabilize ──▶ step limit
            (per strategy)                                   (per direction)
   ──▶ bounds (min/max, schedule) ──▶ cluster guards ──▶ pin / pause ──▶ mode ──▶ write
                                       (quota, capacity,                  (dry-run
                                        rollout rule)                      records "would")
```

| Stage | Default | Records, when it changes the number |
| --- | --- | --- |
| **tolerance** | ±10% of target | `within tolerance (0.66 vs 0.70)` → hold |
| **warmup** | replicas younger than 30s after becoming healthy are not ready; for scale-out they count as 0% busy, for scale-in as 100% (the HPA rule) | `2 of 6 warming` |
| **missing data** | a check with no data contributes nothing, and nothing scales *in* while any check is missing | `held: lag has no data since 14:01 (source team-prom: timeout)` |
| **cold start** | after the app (re)starts, no scale-in until one `down.stabilize` window of samples exists | `held: collecting 5m of history after restart` |
| **stabilize** | up 0s, down 5m. Scale-in uses the *highest* recommendation in the window | `stabilized: 5m max is 6` |
| **step** | up max(+100%, +4)/min, down max(−10%, −1)/min | `rate-limited 7 → 6 (up ≤ 4/min; +2 used)` |
| **bounds** | `min = count`, `max = 4 × count`, with schedule overrides. Bounds sit *after* the step limit on purpose: a raised floor (a schedule's 08:00 `min = 10`, or a resubmit with a higher `count`) applies at once, and so does a lowered `max`. The step limit only paces signal-driven change | `clamped to max 8` (`scale.limited` on first hit) |
| **quota** | the namespace's quota headroom for the job's band | `limited by namespace quota payments-prod (normal: 12/12 cpu)` |
| **capacity** | do not raise the count while more than half of the last scale-out is still unplaced, *unless* a node-group policy can host it | `held: 3 replicas unplaced, no node group fits` |
| **rollout** | scale-in waits for an active rollout to finish (Phase 0; ScaleTask defers it) | `deferred until rollout web-7 completes` |
| **pin / pause** | a pin replaces the result, and may exceed `max` (an operator's explicit override; the note says so). A pause skips the write, and `ScaleTask` also refuses `source = autoscaler` writes to a paused target on the server | `pinned at 12 by alice until 05:10` |
| **flap guard** | 3 direction changes in 10 minutes holds scale-in for `2 × down.stabilize` and emits `scale.flapping` once | `held: flapping (3 reversals in 10m)` |
| **mode** | `enforce` | `dry-run: would scale 4 → 6` |

**Manual scale on an autoscaled task is a pin, not a fight** (calls entry 10).
Phase 0's `legion job scale` keeps working. On an autoscaled task it sets a
time-boxed override that the pipeline treats as final:

```console
$ legion job scale web --count 12 --for 2h --reason "bad deploy"
web/default/web: 6 -> 12; autoscaling pinned until 05:10 (legion job scale web --release)
$ legion job scale web --min 10 --for 1h       # raise only the floor, still autoscaled above it
$ legion job scale web --release
```

On an autoscaled task, `--for` is required. A pin without it is refused with
a hint suggesting `--for 1h`, or removing `autoscale`. An incident pin
should end on its own. When a forgotten pin expires, the default scale-in
rate brings the count down gently instead of all at once.

**Pause.** `legion scaling pause [target | --all] --for 1h --reason …` is
stored on the Control lane and is visible on every status surface. It is
enforced in two places: the app skips paused targets, and `ScaleTask` and
the node-action intents (§6.9) refuse autoscaler writes to a paused target.
A pause therefore holds even against an app that is misbehaving.
`legion scaling resume` undoes it. `--all` is the cluster-wide kill switch.
`legion app disable autoscaler` is the hard stop.

### 6.6 Knowing what scaled and why

Three records, each with a different lifetime.

1. **Decisions that acted are durable.** `ScaleTask` gains a structured
   `decision` (inputs, every stage's note, and a one-line `summary`). The
   manager writes it to a new `scale_history` table in the same Control
   transaction as the override. History is capped per target (default 200
   rows) and covers humans too, so "who changed this and why" has one
   answer.
   A `dry-run` policy's "would scale" decisions are written to the same
   table with `applied = false`. They are as rare as real actions, and
   they are what lets S2 read a week of what the policy would have done.
2. **Evaluations that held are live, not stored.** The app keeps the last
   evaluation per target and a short ring of recent ones in memory. They are
   served by the app's view API (UI, CLI, MCP), and as a summary cached on
   the manager (point 4).
3. **Events for state changes:**
   - `scale.applied`, now carrying `source = autoscaler` plus the summary;
   - `scale.limited` (first time a bound or guard holds);
   - `scale.check_failed` (throttled per check);
   - `scale.flapping`;
   - `scale.pinned`, `scale.paused`, `scale.resumed`;
   - `nodegroup.scale_up | scale_in | join_timeout | provider_failed`.

   There is no event per tick.

```console
$ legion scaling explain web
web/default/web   task   enforce   now 6   bounds 2..8   app: autoscaler (mgr-2) evaluated 4s ago

  check     input                                              wants
  cpu       0.91 of 500MHz, 1m avg over 4 ready (2 warming)       6

  stage      result  note
  merge          6   max of 1 check
  stabilize      6   up: no window
  step           6   up ≤ +4/min; +0 used this minute
  bounds         6   2..8
  decision   4 → 6   applied 14:02:25 (d-81f3)

$ legion scaling history web
TIME      CHANGE   BY          SUMMARY
14:02:25  4 → 6    autoscaler  cpu 0.91 vs 0.70 (4 ready)
13:31:02  5 → 4    autoscaler  cpu 0.41 vs 0.70 for 5m; step −1/min
12:58:40  2 → 5    alice       pinned for 1h: "launch traffic"
12:01:13  -        autoscaler  limited: at max 8 (raise autoscale.max)

$ legion scaling status
TARGET                          KIND   NOW  WANT  BOUNDS  STATE     LAST
web/default/web                 task     6     6  2..8    ok        +2 2m ago (cpu)
payments-prod/api/default/api   task     4     7  4..16   dry-run   would +3 (lag)
workers-general                 nodes    7     9  1..20   scaling   +2 requested, 0/2 joined (1m)
autoscaler: running 1/1 (mgr-2), last evaluation 3s ago
```

4. **The manager knows when the app is gone.** The app sends a heartbeat
   (one cheap `ReportScalingStatus` per tick per shard). It carries each
   target's last evaluation time and a short state, kept in manager memory
   like usage. `legion scaling status` therefore works with the app down,
   and says so: `autoscaler: NOT RUNNING since 14:05 (app job
   legion-system/autoscaler pending: no node in pool legion-system)`. A
   target whose last evaluation is older than three intervals shows `stale`
   everywhere.

**Metrics.** The app serves `/metrics`:

- `legion_autoscaler_evaluations_total{result}`
- `legion_autoscaler_actions_total{direction,source}`
- `legion_autoscaler_desired{target}` (bounded: one series per policy, on
  the app's endpoint, never the manager's)
- `legion_autoscaler_plugin_calls_total{plugin,method,outcome}`
- `legion_autoscaler_plugin_seconds`

Each evaluation is one span (`#[instrument]`, fields `policy`, `target`,
`decision_id`).

**Dry-run against history.** `legion scaling simulate web.ncl --since 6h`
replays the recent rollup history that the status-lane leader keeps (§6.7)
through the policy, and prints the decisions it would have made. The same
engine runs over a CSV for offline testing: `--series load.csv`. This is
what lets an agent or a cautious human change a policy safely.

### 6.7 Inputs: one cluster-wide read

New RPC: **`GetScalingInputs`**. It is batched and served by the
status-lane leader (other managers forward it). For each requested target
it returns:

- ready, warming, and total replicas;
- the sum and per-replica distribution of CPU (in MHz, converted with the
  node's `mhz_per_core`) and memory, against reservations;
- for a service the task backs: request rate, error rate, and latency
  (sum/max) from edges;
- unplaced replicas;
- whether a rollout is active;
- the current override row version, for compare-and-set.

**How the data gets to one place** (calls entry 11):

- Every manager pre-aggregates the reports *it* received into rollups keyed
  by **(node, target)**: sums, counts, maxima, and a small fixed-bucket
  histogram of per-replica utilization, stamped with the report's sample
  time.
- Every 10 seconds (the report interval) it sends the rollups that changed
  to the status-lane leader, on the existing digest RPC.
- The leader keeps the newest rollup per (node, target), whichever manager
  sent it. A node that switches managers is therefore never counted twice:
  the newer sample replaces the older one. The leader then sums per target,
  and keeps one hour of 10-second per-target totals in memory. That buffer
  is what `scaling simulate` replays.
- Size is bounded by (node, target) pairs, roughly one small row per
  autoscaled allocation group on a node, never by raw samples. For 10,000
  autoscaled tasks averaging 3 nodes each, that is about 1.2 MB per manager
  per 10 seconds, and the delta encoding sends much less in steady state.
- Nothing touches raft. A manager that newly takes the status lane asks
  every peer for a **full** resync (not a delta) on its first digest, so
  steady targets don't stay invisible. Until the resync lands, about one
  interval, targets read as missing data, and the missing-data hold (§6.5)
  covers it. The app's own stabilization windows live in the app, so they
  survive a manager failover. Only `scaling simulate` loses history.

The **`legion` source** exposes a fixed, documented vocabulary over these
inputs. A fixed vocabulary is supportable; arbitrary SQL in a policy would
be a footgun and a privilege problem (old plan's lean, kept):

| Query | Meaning |
| --- | --- |
| `task.cpu.utilization` | mean CPU over ready replicas, as a fraction of reservation |
| `task.memory.utilization` | same for memory |
| `task.unplaced` | replicas the scheduler couldn't place |
| `service.rps`, `service.errors`, `service.latency_ms.mean`, `service.latency_ms.max` | from edges; **internal callers only** until the datapath attributes ingress |
| `nodegroup.pending` | nodes of this group's shape needed by pending work (§6.11) |
| `nodegroup.allocated.cpu`, `nodegroup.allocated.memory` | reserved / allocatable across the group |

`service.concurrency` (in-flight requests per replica, Knative's signal) is
a reserved name. It lands when netd attributes ingress requests to the
callee.

### 6.8 Three plugin kinds, one protocol

**The interfaces.** These are Rust traits inside the app, mirrored 1:1 by the
wire protocol:

```rust
trait MetricSource {   // "legion", "prometheus", "http", external…
    async fn query(&self, q: &SourceQuery) -> Result<Reading>;      // value + at, or a series
}
trait Strategy {       // "target-value", "per-replica", "threshold", "pass-through", external…
    fn recommend(&self, r: &StrategyInput) -> Result<Recommendation>; // pure: no I/O
}
trait Target {         // "legion-task" (implicit), "exec", "noop", external ("aws-asg"…)
    async fn status(&self, t: &TargetRef) -> Result<TargetStatus>;  // count, ready, members
    async fn scale(&self, t: &TargetRef, to: &ScaleRequest) -> Result<()>;
    async fn terminate(&self, t: &TargetRef, ids: &[String]) -> Result<()>; // node targets only
    fn capabilities(&self) -> TargetCaps;                             // { nodes: bool, … }
}
```

**Built-ins** need no install and cover S1 through S3:

- sources: `legion` and `prometheus` (instant query plus range for
  `window`; the address is operator config). Also `http`: a GET of a JSON
  URL with a JSON pointer, for "my app exposes `/queue-depth`".
- strategies: `target-value`, `per-replica`, `threshold`, `pass-through`,
  and `fixed`.
- targets: `legion-task` (implicit for `autoscale`), `exec` (one-shot
  shell commands for status, scale, and terminate, in the volume-plugin
  style, through `util::run_json_command`), and `noop` (decisions and
  events only).

**External plugins speak JSON-RPC 2.0, over stdio or HTTP** (calls entry 4).
This is the same framing `legion mcp serve` already uses. The app spawns a
`command` plugin once, keeps it running, restarts it with backoff if it
dies, and applies a per-call timeout (default 10s for sources and
strategies, 120s for targets). A `url` plugin receives the same messages as
HTTP POSTs. That lets a plugin be a Legion service of its own (KEDA's
external-scaler shape), scaled and deployed independently.

```json
→ {"jsonrpc":"2.0","id":1,"method":"initialize",
   "params":{"protocol":"legion-scaling/1","kind":"target","config":{"region":"us-east-1"}}}
← {"jsonrpc":"2.0","id":1,"result":{"name":"aws-asg","version":"0.4.0",
   "capabilities":{"nodes":true},"policyConfigSchema":{ "...": "JSON Schema" }}}

→ {"jsonrpc":"2.0","id":2,"method":"target/status","params":{"policy":"workers-general",
   "config":{"asg":"legion-workers"}}}
← {"jsonrpc":"2.0","id":2,"result":{"count":7,"ready":7,
   "members":[{"id":"i-0abc","node":"w-07","state":"running"}]}}

→ {"jsonrpc":"2.0","id":3,"method":"target/scale","params":{"desired":9,"decision":"d-81f3",
   "join":{"token":"<secret>","attributes":{"legion.nodegroup":"workers-general"}}}}

→ {"jsonrpc":"2.0","id":4,"method":"strategy/recommend","params":{"current":4,"ready":4,
   "reading":{"value":2310.0},"config":{"value":500}}}
← {"jsonrpc":"2.0","id":4,"result":{"desired":5,"note":"2310 / 500 per replica"}}
```

- **One plugin may implement several kinds.** It lists them in
  `initialize`.
- **Secrets on the wire.** A join token rides inside a request body and is
  never logged; the app redacts known secret fields in its traces.
- **Two config levels.** Plugin-level `config` is operator-set and sent at
  `initialize` (credentials references, regions). Policy-level `config` is
  validated by the app against the plugin's `policyConfigSchema`
  (`invalid` status, `scale.check_failed`, and `--wait` on
  `scaling apply`).
- **Testing.** `legion scaling plugin test <command|url>` runs the
  conformance suite: handshake, each declared method, timeouts, and
  malformed replies.
- **SDKs.** A Rust crate (`legion-scaling-plugin`) and two reference
  plugins in other languages (Python, Go) ship in `examples/`.

**Installing, and the trust boundary.** External plugins are listed in the
autoscaler's **app config**, owned by the operator. The app reconciler turns
`source` URLs into artifacts of the app job, so the existing artifact system
fetches, checksums, and caches them into the app's task directory:

```nickel
# legion app config set autoscaler -f autoscaler.ncl
{
  plugins = {
    aws-asg = {
      source = "https://github.com/legion-rs/scaling-plugins/releases/download/v0.4.0/aws-asg-linux-amd64.tar.gz",
      checksum = "sha256:9f1c…",
      config = { region = "us-east-1" },   # credentials: the app's workload identity via OIDC (below)
    },
    team-prom = {
      builtin = "prometheus",
      config = { address = "http://prometheus.internal:9090" },
      namespaces = ["payments-*"],         # only policies in these namespaces may use it
    },
    calendar = { url = "http://calendar-strategy.internal:8080/rpc" },
  },
}
```

**Policies reference plugins by name and never carry a command or a URL**
(calls entry 5). Plugins run inside the app with the app's identity, which
can resize every job. A tenant who could name a command in their policy
could run code as that identity. A tenant who could name a URL could aim
the app's network position at internal endpoints (SSRF). The operator
installs plugins and decides which namespaces may use each one; tenants
supply only data (a query string, a target value). The leftover risk is
stated publicly: a tenant's Prometheus query runs against whatever that
source can see, so give each tenant group its own source if their metrics
must not mix.

**Cloud credentials without static secrets.** The app config can add a
second workload identity to the app job, with the cloud's audience (for
example `sts.amazonaws.com`). It is separate from the reserved `legion`
identity, whose audience is the cluster. OIDC federation exchanges it for
cloud credentials (`AssumeRoleWithWebIdentity` on AWS). The `aws-asg` plugin
receives that token's file path in its `initialize` config, so no access
keys appear anywhere. KV-held secrets remain an option for providers that
cannot federate.

### 6.9 Where it runs, and how it scales

The autoscaler is the bundled app `autoscaler` in `legion-system`. It is
enabled by default and needs nothing configured (apps plan §6.3).

- **Placement.** It runs on the managers' system workers by default. These
  are workers inside the manager process that accept only `legion-system`
  jobs (apps plan §6.2). This placement solves three problems no other one
  does:
  1. It runs on a cluster with zero workers, so node autoscaling can start
     from nothing.
  2. It keeps running when every worker is full, which is exactly when
     scale-up matters.
  3. It never drains the node it runs on during scale-in.

  `legion app set autoscaler --placement workers` moves it. Node scale-in
  then refuses victims that host any `legion-system` allocation.
- **Band** `critical`, so it is never preempted.
- **One replica by default.** Losing it pauses scaling until the
  reschedule, and every surface says so (§6.6).
- **Horizontal scale by sharding** (calls entry 13). Policies hash to 64
  shards. Each replica keeps a membership lease
  (`legion/apps/autoscaler/members/<alloc>`) and holds shard leases
  (`…/shards/<n>`) up to `ceil(64 / live members)`, rebalancing as members
  come and go. Both kinds of lease are KV locks with a 15s TTL.
  `legion app set autoscaler --replicas 3` is the whole procedure.
  The `legion/` KV prefix is reserved the same way `legion-*` namespaces
  are (apps plan §6.2): a `paths = ["*"]` rule doesn't reach it. So a
  tenant with broad `kv:write` can't take or starve shards.
- **No duplicate actions.** KV locks have no fencing token
  (`store.rs:19343`), so two replicas can briefly both believe they own a
  shard. Every write the app makes is therefore **compare-and-set** against
  replicated state, so a stale owner loses (calls entry 12):
  - **Task targets.** `ScaleTask.expected_version` names the override
    version the decision was computed from. A stale writer gets
    `FAILED_PRECONDITION`, re-reads, and re-evaluates. The same check stops
    the autoscaler from overwriting a human's scale made a second earlier.
  - **Node and external targets**, whose side effects happen outside
    Legion. Before calling `target/scale`, choosing drain victims, or calling
    `target/terminate`, the app records an **action intent**
    (`RecordScalingAction`: policy, desired count or victim ids, decision
    id) with CAS on the policy's action version. Only the winner of that
    write calls the plugin. The loser sees the intent and adopts it as
    in-flight state. Intents carry their outcome when the plugin call
    returns. An intent left open by a crashed replica is reconciled from
    `target/status` by the next owner before it acts.
  - **Versions never repeat.** They are the raft log index of the row's last
    write, not a per-row counter. Arbitration deleting and recreating an
    override (Phase 0) therefore can't produce an ABA match.
- **Reads are batched and pushed.** Policies come from
  `WatchScalingPolicies`, a stream filtered by shard: changes arrive
  immediately, and there is no polling. Inputs come from one
  `GetScalingInputs` call per shard per tick. The default tick is 15s,
  overridable per policy with `interval`.
- **Cost.** For 10,000 task policies at 15s with built-in sources, the
  manager sees one batched read and a heartbeat per shard per tick, plus
  writes only on change. External sources dominate cost: the app dedupes
  identical queries within a tick, caps concurrency per plugin, and
  sharding spreads the rest. Phase 7 makes "10,000 policies on one replica"
  an acceptance number.

### 6.10 Interactions the evaluator must respect

- **Rollouts.** The ECS rule from Phase 0 is enforced by `ScaleTask`, so
  the app cannot bypass it. A deferred scale-in shows `deferred`, not
  `ok`.
- **Stateful slot tasks** scale in highest-slot-first and honor
  `retainOnScaleIn` (Phase 0). Keyed and `count.per` tasks are rejected at
  submit (non-goals).
- **Gangs.** `autoscale` on a `gang` task requires `min` ≥ the gang's
  quorum. A scale-out that would leave the gang short simply blocks like any
  gang placement.
- **Preemption.** A scale-out in a higher band can preempt lower bands.
  That is the band system working, and the decision note says
  `placed by preempting 2 low-band allocations`.
- **Namespace quotas** cap scale-out (the quota stage) instead of letting
  the count climb past what admission will place.
- **Suspend/idle** applies to keyed instances, which can't be autoscaled,
  so the two never meet in v1.
- **Session envelopes** sit one per node and are never "empty". Node
  scale-in treats a node whose only allocations are envelopes with zero
  live sessions as empty (§6.11).
- **Oversubscription ratios** are part of node capacity in the demand
  simulation.

### 6.11 Node autoscaling

A node-group policy is a standalone policy object:

```nickel
# nodes.ncl  —  legion scaling apply nodes.ncl
{
  name = "workers-general",
  target = {
    nodes = {
      pool = "default",
      workerType = "default",
      # Needed to scale from zero, or before any member has joined. Once a
      # member exists, its observed capacity and attributes are used instead.
      shape = { cpu = 8000, memory = 16384, attributes = { arch = "amd64" } },
      plugin = "aws-asg",
      config = { asg = "legion-workers" },
    },
  },
  min = 1,
  max = 20,
  up   = { step = { replicas = 5 }, every = "2m", joinTimeout = "10m" },
  down = {
    when = "empty",          # empty (default) | underutilized
    for = "10m",
    step = { percent = 10, replicas = 1 },
    every = "5m",
    drainDeadline = "10m",
  },
  expander = "least-waste",  # when several groups could host the same work
}
```

**Scale-up comes from pending work, simulated by the manager** (calls entry
7). New RPC **`GetCapacityDemand`**:

1. It takes the jobs short of their effective count (the capacity pass's
   SQL), plus blocked gangs and their quorum.
2. For each node-group template, it builds a synthetic node: the shape,
   the pool, attributes, and the oversubscription ratios.
3. It answers "how many of these would the pending replicas need?" with
   the scheduler's own feasibility functions (`is_permitted`, constraints,
   `distinct`, static ports, resources) and first-fit-decreasing packing.
4. Replicas no template can host are reported as `unfittable`, with the
   failing rule. That is a jobspec or policy problem, not a capacity
   problem.

This runs read-only on whichever manager answers. It does not touch the
scheduler's hot path, and it is computed from the same rules the scheduler
enforces, so the autoscaler cannot disagree with the scheduler about what
fits. `nodegroup.pending` (§6.7) is its output.

**Scale-up flow:**

1. The app mints a join token scoped to the group. `CreateJoinToken` gains
   `max_uses` and `attributes`, stamped on every node that registers with
   it (`legion.nodegroup = workers-general`).
2. It calls `target/scale` with the desired count and the token.
3. Each instance joins with
   `legion join --token-file … --attr legion.provider-id=$(instance-id)`.
   `--attr` is new; per-host values live there.
4. Nodes that have not joined within `joinTimeout` are reported as
   `nodegroup.join_timeout`, the group backs off, and the count the
   provider holds is reconciled from `target/status`.

**Scale-in flow:**

1. **Victims.** With `when = "empty"`, a node is a victim once it has had
   no allocations for `for` (ignoring session envelopes with no sessions,
   and allocations of `count.per = "id"` per-node tasks). With
   `underutilized`, its reserved share must be under 50%, and a simulation
   must show every allocation on it fits elsewhere.
2. **Never victims:**
   - a node with a `legion-system` allocation;
   - a node holding stateful node-local storage;
   - a node tagged `legion.scale-in = "never"`;
   - a node whose drain would break a `disruption.minAvailable`.
3. **Budget.** At most `step` victims per `every`.
4. **Drain.** Cordon and drain with `SetNodeAvailability`, wait until the
   node has no allocations, or until `drainDeadline` passes (then uncordon
   and emit `nodegroup.scale_in` with `aborted`).
5. **Terminate and forget.** `target/terminate([provider-id])`, then the
   new **`ForgetNode`** RPC removes the record. `legion node forget` is
   also usable standalone for dead records, and a lost-node GC option is
   tracked separately in TODO.

The drain is Legion's job, not the plugin's: one tested path, and every
plugin only has to know how to stop an instance.

**Pools are already taints**, so a node group is a pool plus a template, and
the demand simulation honors permits exactly. The old plan's caveat that
"pools are scheduling-inert" no longer applies.

### 6.12 Scale to zero

`min = 0` is allowed only if some check can wake the target, meaning a
check with `activation` on a source that exists at zero replicas. Waking
from zero always takes the activation path; scaling from 1 to N uses normal
checks (KEDA's split). Validation rejects the deadlock:

```
error: web/default/web: autoscale.min = 0 needs a check that can wake it from zero.
  'cpu' reads the task's own usage, which is empty at 0 replicas.
  hint: add a check with `activation` on an external source or a schedule, or set min >= 1
```

The idle side reuses the down pipeline. A target reaches zero only after
every check reports below activation for `down.stabilize`.

**Reserved for the gateway.** When the gateway can route to services, it
will call `ScaleTask` (source `wake`) on the first held request, and the
policy surface will gain `wake = "on-request"`. This plan names the hook and
ships nothing for it.

### 6.13 Access control

- **The app's grant** (declared in its manifest, apps plan §6.4):
  - `scale:read,write` and `job:read` (all namespaces);
  - `node:read,write` (drain, forget, join tokens);
  - `kv:read,write` on `legion/apps/autoscaler/*` (leases).

  It does **not** get `job:write`: it can resize, not redeploy. It does get
  `node:write`, even on clusters with no node-group policies. The calls doc
  (entry 18) explains why the grant is not split by feature.
- **Standalone policies.**
  - A task or external-target policy needs `scale:write` in its namespace.
  - A node-group policy is cluster-scoped. It needs `node:write`, which is
    cluster-scoped and has no namespace to name, and `operator:write`,
    because it can add and remove machines. A job owner with broad
    `scale:write` therefore cannot create one.
  - Plugin use is gated by the plugin's `namespaces` allowlist.
- **Pause scope.** Pausing one target needs `scale:write` on that target's
  namespace. `--all` and node-group pauses need `operator:write`.
- **App-plane RPCs.** `ReportScalingStatus` and `RecordScalingAction` are
  marked `// ACL: app-plane`. They accept only the autoscaler app's own
  workload identity (principal `legion:<legion-system/autoscaler sub>`),
  the same way worker-plane RPCs accept only worker credentials. A human
  with `scale:write` can't forge the app's status or intents.
- **`ForgetNode`** revokes the node's token in the same write, so an
  instance whose termination failed cannot re-register under the old
  identity. It would have to join again with a new token, and the node
  group's `max_uses` join token is already spent.
- **Every new RPC** gets a row in `tests/integration/acl_endpoints.rs`:

  | RPC | ACL |
  | --- | --- |
  | `GetScalingInputs` | `scale:read` |
  | `GetCapacityDemand` | `node:read` |
  | `WatchScalingPolicies` | `scale:read` |
  | `ApplyScalingPolicy`, `DeleteScalingPolicy` | as above |
  | `ListScalingPolicies` | `scale:read`, filtered |
  | `ReportScalingStatus`, `RecordScalingAction` | app-plane (the autoscaler's identity only) |
  | `GetScalingStatus`, `GetScaleHistory` | `scale:read`, filtered |
  | `PauseScaling` | `scale:write` on the target; `operator:write` for `--all` and node groups |
  | `ApplyScalingDefaults` | `operator:write` |
  | `GetScalingDefaults` | `scale:read` |
  | `ForgetNode` | `node:write` |

  Coarse checks come first, then the precise scope, so a probe cannot tell
  NotFound apart from PermissionDenied.

### 6.14 UI, CLI, MCP

- **UI**, built as app extensions (apps plan §6.5), not hand-written SPA
  pages:
  - a declarative **Autoscaling** panel on the job page (now / want /
    bounds, a one-hour series of replicas against the signal, a timeline of
    decisions, Pause and Release buttons);
  - a badge on each autoscaled task row (`auto 6 · 2..8`);
  - an **Autoscaling** page in the sidebar listing every policy;
  - a node-group panel on the Nodes page.

  The same views render in the terminal with `legion app view autoscaler
  job web`.
- **CLI**: `legion scaling list | status | explain | history | apply |
  delete | pause | resume | simulate | plugin test`, plus the new
  `legion job scale … --for/--min/--max/--release` flags and
  `legion node forget`. Every command takes `--json` and goes through
  `output::resolve`, and each one is catalogued in
  `src/cmd/capabilities.rs`.
- **MCP**:
  - read-only, always listed: `legion_scaling_status`,
    `legion_scaling_explain`, `legion_scaling_history`, and
    `legion_scaling_simulate`;
  - mutating (behind `--allow-write`): `legion_scaling_pause` and
    `legion_scaling_apply`.

  `ApplyScalingPolicy` and `PauseScaling` take `idempotency_key`, and
  `scaling apply --dry-run` validates and diffs without writing.

## 7. Proto, struct, config, and persistence changes

**Proto (`proto/legion.proto`), every message and field with public comments**

- `ScaleTaskRequest`:
  - `+ optional uint64 expected_version` (compare-and-set);
  - `+ ScaleDecision decision` (summary, stages, inputs; stored in
    `scale_history`);
  - `+ string source` (`operator | autoscaler | wake`; the server ignores
    `autoscaler` and `wake` from tokens without the app's identity, and
    records them);
  - `+ optional Duration pin_for`, `optional uint32 min`,
    `optional uint32 max`, `bool release`.
- `TaskScaleStatus` `+ policy summary`, `pin`, `paused`, `last_evaluation`,
  and `state`.
- New RPCs:
  - `GetScalingInputs`, `GetCapacityDemand`, `WatchScalingPolicies`
    (server stream);
  - `ApplyScalingPolicy`, `DeleteScalingPolicy`, `ListScalingPolicies`;
  - `ApplyScalingDefaults` (with `dry_run` and `force`, and an idempotency
    key), `GetScalingDefaults` (effective defaults for a namespace, with
    provenance);
  - `ReportScalingStatus`, `RecordScalingAction` (app-plane),
    `GetScalingStatus`, `GetScaleHistory`;
  - `PauseScaling`, `ForgetNode` (also revokes the node token).
- `CreateJoinTokenRequest` `+ optional uint32 max_uses`,
  `map<string,string> attributes`.
- `RegisterWorker` `+ map<string,string> join_attributes` (from `--attr`).
- The digest RPC gains a `repeated TargetRollup rollups` (internal,
  manager-plane).

**Structs / store**

- `TaskConfig` `+ autoscale: Option<Autoscale>`, which is either a boolean or
  a record (serde untagged, camelCase, schemars). It is persisted **as
  written** (not expanded) inside the existing `task_config` JSON. `min` and
  `max` accept a number or an `"Nx"` multiple of `count`.
- An effective-policy resolver in the manager (built-in, then cluster
  defaults, then matching namespace policies, then the task). `job plan`,
  `WatchScalingPolicies`, `scaling explain --policy`, and validation all use
  it. It is re-run for inheriting policies when any level changes.
- `NamespacePolicy` `+ autoscale` (the same record shape, with no `checks`),
  using the existing all-policies-apply rule.
- The in-place comparator: for autoscaled tasks, a resubmit that differs
  only in `autoscale` and `count` takes the
  `migrate_allocations_to_job_config` path.
- New tables on the Control lane:
  - `scaling_policy` (name, namespace, version, spec JSON, created/updated
    by);
  - `scale_history` (target key, at, from, to, source, principal, summary,
    decision JSON; capped per target);
  - `scaling_pause` (target or `*`, until, reason, principal);
  - `scaling_defaults` (one versioned row: the cluster-level record,
    updated by, updated at).

  Pins live on `task_scale` as `pin_until`, `pin_min`, and `pin_max`
  columns, so `effective_task_count` stays the single reader and the
  scheduler needs no new lookup.
- `task_scale.version` is the CAS token: the raft log index of the row's
  last write, so it never repeats across a delete and recreate.
- `scaling_action` (policy, action version, kind, desired or victim ids,
  decision id, state, outcome) holds the node and external intents (§6.9).
- `scale_history` gains `applied` (false for dry-run "would" rows) and
  `policy_digest` (the effective policy the decision used).
- `handle_job_submit_apply` gains the autoscale-aware arbitration from §6.3:
  keep the override, rebase `base_count`, and clamp into the new bounds.
- `validate_job_config` gains autoscale validation:
  - `min ≤ count ≤ max`;
  - the min-zero wake rule;
  - the usage-capable runtime check;
  - group rules;
  - the gang quorum;
  - keyed and `count.per` rejection.

**Config**

- `configs/jobs/maximal.ncl`: `autoscale = true` on one task, and the full
  record with `checks`, `schedule`, and `mode` on another.
- `configs/scaling/nodes-aws.ncl` and `configs/scaling/nodes-exec.ncl` as
  examples.
- The autoscaler's app config schema is shipped in the binary and printed by
  `legion app config schema autoscaler`.
- Worker: none (the usage interval stays 10s).

## 8. Implementation phases

Each phase ships alone and is useful alone. Phases 2 onward need the apps
plan's phases A1 to A3 (system worker, grants and ambient credentials, the
app framework). If those slip, Phase 2 ships as a documented job template
with a hand-written identity binding, with no change to the engine.

0. **Shipped:** `legion job scale`, count overrides, the `scale` ACL
   resource (2026-07-30).
1. **Cluster-wide inputs.**
   - Per-target usage and edge rollups on the digest;
     `GetScalingInputs`.
   - Request-vs-usage in `legion job info` and the job page.
   - `GetJobScaleStatus` reads through `effective_task_count` (hardening).

   *Alone:* "how busy is my job" has one answer on any manager.
2. **Task autoscaling MVP as the bundled app.**
   - The engine, with the `legion` source, `target-value`, and the
     `cpu`/`memory` shorthands.
   - `autoscale = true` and its expansion in `job plan`.
   - Layered defaults (cluster `scaling defaults`, namespace-policy
     `autoscale`, the task), resolved live, with provenance in
     `scaling explain --policy`. In-place resubmits for `autoscale` and
     `count` changes.
   - The full safeguard pipeline, compare-and-set writes, decisions in
     `scale_history`, `ReportScalingStatus`.
   - CLI: `legion scaling status | explain | history | pause | resume`;
     pins on `job scale`.
   - Events, `dry-run` mode, MCP read tools.

   *Alone:* S1 and S5, plus S2's bounds, step limits, and dry-run week.
   S2's business-hours floor needs schedules (Phase 3).
3. **Full policies.**
   - `checks`; the `prometheus` and `http` sources; the `per-replica`,
     `threshold`, `pass-through`, and `fixed` strategies.
   - Schedules, `headroom`, `scaleIn = false`, `onMissing`.
   - `min = 0` with activation and its validation.
   - `legion scaling simulate` over the rollup buffer and CSV.

   *Alone:* S3, and S2 in full.
4. **External plugins and standalone policies.**
   - The JSON-RPC protocol over stdio and HTTP; plugin install through app
     config (artifacts).
   - The `exec` and `noop` targets, `legion scaling plugin test`, the Rust
     SDK and two example plugins.
   - `ScalingPolicy` objects with external targets, with `apply`, `list`,
     and `delete`.

   *Alone:* S6, and scaling non-Legion systems.
5. **Node autoscaling.**
   - `GetCapacityDemand`; node-group policies.
   - Join-token `max_uses` and `attributes`; `legion join --attr`.
   - The drain → terminate → forget flow with budgets and protections;
     `ForgetNode` and `legion node forget`; the expander.
   - A first-party `aws-asg` plugin as its own artifact.

   *Alone:* S4.
6. **UI as app extensions.** The job panel, task badge, sidebar page, and
   node-group panel; `legion app view`. This needs apps phase A4 and can
   land any time after Phase 2.
7. **Scale-out of the autoscaler itself.** Shard leases, rebalancing,
   per-plugin concurrency caps and query dedupe, a 10,000-policy load test
   with published numbers.

Deferred, each a TODO entry: gateway wake for services (`wake =
"on-request"`), `service.concurrency` once netd attributes ingress, keyed
instance autoscaling, `count.per` autoscaling, consolidation, predictive and
vertical strategies, and WASM strategies.

## 9. Testing plan

Grouped by the phase that adds them. Every phase also runs the regression
gates at the end.

**Phase 1: inputs**
- Rollup merge across three managers with one stale.
- A node switching managers is counted once (the newest per-(node, target)
  sample wins).
- After a status-lane leader change, the full resync arrives, and inputs
  read as missing data until it does.
- Size bounds; MHz conversion; `cores` reservations.
- `GetJobScaleStatus` agrees with `effective_task_count` for every
  override state, including pins.

**Phase 2: engine and task autoscaling**
- Pipeline unit tests, table-driven:
  - every stage's pass and change behavior;
  - the HPA formula and tolerance band;
  - the warmup rule (0% up, 100% down);
  - missing-data holds, `onMissing = ignore`, the cold-start hold, and the
    flap guard;
  - rolling `step` windows, including both directions within one window,
    and a partial `step` record zeroing the other half;
  - bounds after step (a raised floor applies at once);
  - the pin and pause precedence matrix, including a pin above `max`;
  - the defaults matrix:
    - the precedence of built-in, cluster, namespace, and task;
    - nested merges, and `step` replacing the record below it;
    - `"Nx"` resolution against `count`;
    - signals being all or nothing;
    - two namespace policies setting one field differently, refused with
      both named;
    - a defaults change delivered live through `WatchScalingPolicies`;
    - an invalidating defaults change refused, and with `--force` holding
      the affected policies as `invalid`;
    - provenance output naming the right level for every field.
- Engine simulation tests. A deterministic clock plus synthetic series
  (ramp, spike, sawtooth, a dropped source) are run through the same code
  path `legion scaling simulate` will use. They assert the decision
  sequence, and assert that no oscillation exceeds the flap guard. S1's
  numbers become golden tests.
- Store and RPC:
  - CAS conflicts: two writers, and human vs autoscaler;
  - version tokens survive a delete and recreate without repeating;
  - `scale_history` cap, ordering, and dry-run rows;
  - pins expiring through `effective_task_count`;
  - `ScaleTask` deferral during rollouts with `source = autoscaler`;
  - server-side refusal of autoscaler writes to a paused target;
  - group targets writing every member in one transaction;
  - the autoscale-aware submit arbitration (raise count, lower count,
    remove `autoscale`), as a matrix next to Phase 0's;
  - a resubmit changing only `autoscale` or `count` keeps the same
    allocation ids, with no rollout.
- Integration (`tests/integration/autoscaler.rs`, one module in the shared
  binary, using worker simulation for fake usage):
  - S1 end to end: load ramps, the count rises, the count falls after the
    window;
  - the app on the manager's system worker with zero workers;
  - app death: `scaling status` reports NOT RUNNING, and nothing scales;
  - `ReportScalingStatus` from a non-app token is refused.

**Phase 3: full policies**
- Each built-in strategy's math, with schedule time zones and DST.
- `min = 0` validation and the activation path.
- `scaling simulate` over the live rollup buffer matches the decisions the
  engine made over the same hour.

**Phase 4: plugins and standalone policies**
- The conformance suite (`scaling plugin test`) runs in CI against the
  example plugins: crash and restart, a timeout, a malformed reply, a
  schema-invalid policy config.
- A tenant policy naming an uninstalled plugin, or one outside its
  namespace allowlist, is rejected at apply.

**Phase 5: nodes**
- Node groups against a scripted `exec` provider from zero, including:
  - `joinTimeout`;
  - drain → terminate → forget ordering, and the drain-deadline abort;
  - the never-victim protections;
  - `ForgetNode` revoking the node token.
- Two app replicas with a forced lease overlap: exactly one action intent
  wins, and exactly one `target/scale` call reaches the provider.

**Phase 7: sharding**
- Rebalance after a replica is killed.
- Exactly one `ScaleTask` lands per decision under a lease overlap.
- The 10,000-policy load run.

**Regression gates (every phase)**
- `acl_endpoints::every_manager_rpc_is_classified` for each new RPC.
- The capability-catalog test (`mutates` flags).
- Heartbeat-path benchmarks before and after the rollups (Phase 1).
- `GetCapacityDemand` reuses scheduler feasibility functions but is not on
  the placement path. Any refactor that touches `is_permitted` or candidate
  filtering ships with `scheduler_bench` before/after numbers (AGENTS.md).

## 10. Rollout and compatibility

- **Default-on, inert until used.** The autoscaler app is enabled on every
  new cluster, and with no `autoscale` blocks and no policies it idles at a
  watch stream and a 15s tick. Clusters with the system worker disabled run
  it on workers.
- **What breaks, deliberately** (AGENTS.md says break things for better
  defaults, and the PR will say so):
  - **`count` changes meaning on autoscaled tasks.** The old plan made it
    "initial only". Here it is the default `min`.
  - **`legion job scale --count` on an autoscaled task now requires
    `--for`.** It used to have no autoscaled tasks to meet.
  - **The old plan's `scaling` field name never shipped.** The field is
    `autoscale`.
- **No data migration.** The new tables start empty. `task_scale` gains
  nullable pin columns and a `version` column (default 0).
- **Version skew.** Old workers send no rollups. They already send usage,
  and rollups are built on managers. An old manager in the cluster sends no
  rollups on the digest, so its nodes' usage is missing at the leader.
  Missing data holds, so a mixed-version cluster scales less, never wrongly.
  This fits `docs/upgrades.md`.
- **Docs, in the same change as each phase:**
  - `docs/autoscaling.md`, rewritten from "the manual half" to the whole
    feature: the surface, the defaults table, the pipeline, every failure
    mode, and limitations including internal-only RPS, no service wake, and
    governed runtimes only;
  - `docs/scaling-plugins.md`, the protocol reference;
  - `docs/node-autoscaling.md`;
  - updates to `docs/agent-readiness.md`, `docs/event-stream.md`,
    `docs/metrics.md`, `docs/auth-and-acls.md`, and
    `docs/plugin-system.md` (a pointer to the scaling protocol);
  - the `README.md` doc list and the `docs/README.md` index.

## 11. Open questions

- **Should the default include a memory guard?** Lean: no. The oscillation
  analysis in calls entry 2 says a memory check that cannot vote down
  creates up/down fights with CPU. We would revisit with per-replica PSI
  (`psi_memory_some_avg10` is already sampled) as a pressure signal rather
  than a utilization one.
- **Should the default `max` be `4 × count` or a cluster setting?** Lean:
  keep the formula, and let namespace policies set a `maxAutoscale` ceiling
  for tenants who want one.
- **Should the app autoscale itself** (on evaluation lag)? Lean: possible
  once sharding exists. It needs a `legion` query `app.autoscaler.lag`.
  Not before Phase 7.
- **Should pins have a no-expiry form** for planned events? Lean: no.
  `--for 72h` is explicit, and a schedule entry is the right tool for
  anything recurring.
- **Lost-node GC.** It belongs with node lifecycle, not here, but node
  scale-in makes the gap visible. Tracked as a TODO item with a lean
  toward `node.gcAfter` (default off).

## 12. Acceptance criteria

- **P1.** `GetScalingInputs` returns the same per-task CPU for a job whose
  allocations heartbeat to three different managers, on every manager. A
  node switching managers mid-test is not double-counted. Usage is visible
  in `legion job info` and the job page.
- **P2.**
  - The S1 jobspec, unchanged, holds a CPU-loaded task within the
    tolerance band through a ramp, scales out within one minute, and
    scales in no faster than the defaults.
  - `legion scaling explain` names the stage that held each non-action.
  - A manual `job scale --count` without `--for` on an autoscaled task is
    refused with the documented hint.
  - Killing the app shows NOT RUNNING within three intervals.
  - `--all` pause stops all writes.
  - Overriding defaults:
    - `autoscale = { max = 20 }` changes only `max`;
    - one `scaling defaults apply` changes every inheriting task's effective
      policy without a resubmit;
    - `scaling explain --policy` names the source of every field;
    - raising `max` in the jobspec doesn't restart the task.
- **P3.** S3 sleeps at zero and wakes on lag. `min = 0` without activation
  fails validation with the documented message. `scaling simulate` over 1h
  of history matches the decisions the live engine made in that hour.
- **P4.** A Python strategy from `examples/` passes `scaling plugin test`.
  It is installed through app config and used by a policy. A tenant policy
  naming it from a namespace outside its allowlist is rejected at apply.
- **P5.** With the scripted provider, sustained unplaceable work provisions
  the simulated node count from zero. The nodes join carrying the group
  attribute and the provider id. Empty nodes drain, terminate, and vanish
  from `legion node list` at the budgeted rate. A node hosting
  `legion-system` is never chosen.
- **P6.** The job page shows the Autoscaling panel for an autoscaled job.
  It renders nothing for a viewer without `scale:read` on that job. The
  same view renders in the terminal.
- **P7.** One app replica sustains 10,000 task policies at a 15s interval
  with built-in sources (numbers published in `docs/benchmarks.md`). Three
  replicas rebalance shards within one lease TTL after one is killed, with
  no duplicate writes.
