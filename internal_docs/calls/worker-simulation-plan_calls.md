# Judgement calls: worker-simulation-plan

Plan: [`../plans/worker-simulation-plan.md`](../plans/worker-simulation-plan.md)

Entries are ranked by project impact; the number is the rank.

## 1. Sim nodes speak the unmodified worker-plane wire, at real-worker cadences

**Call.** Simulation adds no RPCs, no messages, no manager-side "fake
node" awareness, and no cadence shortcuts: a sim node registers,
heartbeats, and reports health with the same messages, intervals, retry
backoffs, and re-register-on-`Unauthenticated` behavior as
`src/worker/daemon.rs`. The only manager-visible difference is
`"simulated": true` in `config_attrs`.

**Alternatives.** (a) A bulk `RegisterSimulatedNodes(N)` RPC — far
cheaper to drive and what a pure load-generator would do. (b) Faster
heartbeat cadence as a stress knob.

**Trade-off.** Ramp-up at 10k nodes takes minutes at the default
`--register-rate` instead of one RPC, and stress-testing beyond real
cadence needs more sim processes rather than a knob.

**Why (performance, honestly measured).** The AWS campaign's claim is
that the manager was measured under N *wire-real* nodes. A bulk
registration path or synthetic cadence would measure a code path no real
cluster runs, and it is exactly the shortcut a benchmark critic would
look for. Fairness to Nomad/K8s also demands it: nodesim and KWOK both
speak their systems' real node protocols.

## 2. `--simulate N` is a flag on `start worker`, not a `legion nodesim` verb

**Call.** The AWS plan left this open ("or a dedicated `legion nodesim`
subcommand — decide in its own mini-plan"). Decided: extend the existing
verb.

```bash
legion start worker $TOKEN --simulate 1000 --profiles fleet.json
```

**Alternatives.** `legion nodesim $TOKEN -n 1000` — nomad-nodesim's
shape, a separate tool that could grow fault-injection flags without
polluting the worker surface.

**Trade-off.** `start worker`'s help now carries four flags most
operators will never use, and a future dedicated simulator UX (fault
injection, per-node scripting) would force either flag sprawl here or a
migration to the verb we declined.

**Why (simplicity).** One verb for "attach worker(s) to a cluster" is
easier to explain than two; the token flow, config resolution, and docs
are shared rather than duplicated; and the capability catalog keeps one
entry. The rejected verb is cheap to add later if fault injection ever
becomes real — the engine is a module (`src/worker/simulate.rs`), not
CLI-shaped.

## 3. One shared stub `Worker` server per sim process, so push dispatch stays measured

**Call.** Each sim process serves a single tonic `Worker` endpoint and
advertises it as the `workerAddress` of *every* node it simulates.
`PostWork` routes on the payload's `node_id` and accepts the
allocation; `GetTaskLogs`/`ExecTask`/`RestartWorker` answer
`Unimplemented`.

**Alternatives.** (a) No server and no `workerAddress` — the initial
lean, simplest possible process. (b) One listener per sim node —
maximally realistic addressing.

**Trade-off.** Against (a): a listener and port to manage per process.
Against (b): the manager sees ~one address per instance instead of one
per node, so per-node connection-establishment cost is understated
(shared-channel reuse); accepted and recorded in the fairness notes —
nodesim and KWOK make the same trade.

**Why (performance, honestly measured).** Without an address the
dispatcher *skips* push dispatch entirely
(`src/manager/dispatcher.rs:43-49`, "worker address missing") — with a
skip log per placement, at campaign volume a log flood — and the
benchmark would silently exempt Legion's managers from a real
per-placement cost that every production cluster pays. Reversed from
the plan's first draft precisely because the code reads that way, not
how the draft assumed.

## 4. A sim fleet reports as one worker in the shared registry, with no per-node series

**Call.** A `--simulate N` process starts the same opt-in worker
telemetry listener a real worker does (extracted as
`daemon::start_worker_telemetry`, called from both paths) and reports
into the same process-global `legion_worker_*` registry. The existing
heartbeat and allocation families become fleet-wide aggregates; three new
ones — `legion_worker_simulated_nodes`, `..._nodes_target`,
`..._registrations_total{outcome}` — describe the fleet. No metric is
labelled by node.

```bash
LEGION_WORKER_TELEMETRY_PORT=9101 legion start worker "$TOKEN" --simulate 1250
curl -s localhost:9101/metrics | grep legion_worker_
```

**Alternatives.** (a) Give the simulator its own `/metrics` endpoint and
its own `legion_sim_*` families — the TODO offered this explicitly.
(b) Export per-node series (`legion_worker_heartbeats_total{node="..."}`)
so a fleet is inspectable node by node.

**Trade-off.** Against (a): the shared families now mean "per process,
which may be N nodes", so their help text and docs must carry that
caveat, and a dashboard mixing real and simulated hosts must not sum
them naively. Against (b): you cannot ask a sim host about one specific
node — you have to ask the manager.

**Why (simplicity/ease).** A campaign should scrape one set of families
and get one answer whichever kind of host produced it; `benchmarks/aws/`
already provisions Prometheus jobs and Grafana boards against
`legion_worker_*`, and a parallel `legion_sim_*` namespace would fork
every panel and every recording rule. Sharing the listener also means the
real and simulated paths cannot drift on *how* telemetry is switched on
— the thing that broke here in the first place, when `--simulate` and the
worker-telemetry work merged past each other and left the campaign's own
hosts exporting nothing. Per-node series were never on: at the ~1,250
nodes per host the rig packs, four families × 1,250 is a cardinality bomb
aimed at the box measuring the experiment, and the manager already holds
the authoritative per-node view.

## 5. N is distributed over profiles proportionally, not taken from profile counts

**Call.** `--profiles` supplies *weights* (the schema's `count` fields);
`--simulate N` supplies the total, split largest-remainder. The same
`web_fleet.json` therefore drives every scale step (1k/2k/5k/10k) and
each sim process runs its slice with identical shape.

**Alternatives.** Honor the file's counts literally (a scenario file
"means what it says") and require per-step profile files.

**Trade-off.** A profile file no longer pins an exact fleet: rounding
can shift a node between classes across steps, and reproducing a step
means recording N alongside the file (the campaign archive does).

**Why (ease).** The campaign's whole axis is "same shape, different N";
per-step files are four copies that drift. Literal counts remain
available trivially (set N to the file's total).

## 6. Restart-safety comes from deterministic names, not persisted state

**Call.** Sim nodes have no state directory; `<prefix>-<profile>-<idx>`
naming makes a restarted process re-register the same node ids
(registration is an upsert on the manager).

**Alternatives.** A state file per process recording issued node tokens,
like the real worker's state dir.

**Trade-off.** A restarted process loses node tokens and re-registers
with the join secret — N registration writes instead of N token reuses —
and any allocations the manager believed running on those nodes flap
lost→re-placed. Accepted: that is also exactly what the storm scenario
does on purpose.

**Why (simplicity).** Simulation state that survives its process is
state nobody asked for; the failure mode ("restart = fleet blip") is the
same event the benchmark already models as churn.

## 7. The fleet's allocation gauge is delta-maintained and repaired on a cadence, not recomputed per heartbeat

**Call.** `legion_worker_allocations_running` on a sim host is kept
current by folding in only the *change* against what each node last
reported (O(1) per heartbeat), with a re-derivation from the
authoritative per-node state every 30s on the existing summary cadence.
The repair sums what nodes last reported rather than what they currently
hold — the same quantity the deltas maintain — so it cannot itself
desync the total it exists to fix.

**Alternatives.** (a) Recompute the fleet total on every heartbeat
(`SimFleet::acked_total`, already there for the status line).
(b) Delta-only, with no periodic repair.

**Trade-off.** A scrape can disagree with the manager by a few
allocations under heavy churn, and the drift window is one summary
interval. Documented as a limitation in both public pages rather than
left for a reader to discover from a graph that does not quite add up.

**Why (performance).** (a) walks every node and takes every node's
allocation lock: at 1,250 nodes heartbeating on a 10s cadence that is
~156k lock acquisitions per second, spent by the process whose spare
capacity is the thing making the measurement trustworthy — the
instrumentation would become the load. (b) is cheap but lets the racy
read-modify-write on a gauge accumulate error over a 24h soak, which is
exactly the run where a slowly-wrong number is most expensive to
discover. Counters and the histogram take neither compromise: they are
exact.
