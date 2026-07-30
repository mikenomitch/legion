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

## 4. N is distributed over profiles proportionally, not taken from profile counts

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

## 5. Restart-safety comes from deterministic names, not persisted state

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
