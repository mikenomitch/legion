# Plan: The Isolate Runtime: Dense Multi-Tenant Isolates That Burst to a VM

> **Judgement calls:** [isolate-runtime-plan_calls.md](../calls/isolate-runtime-plan_calls.md)

**Status: PLANNED** (written 2026-10-28). Nothing here is built.

This plan adds a second, much lighter tier to Legion's sandboxing. Code runs
first as an **isolate**: a WebAssembly instance inside a shared,
multi-tenant host process, started in microseconds, holding no thread while
it waits on I/O, and costing tens to hundreds of kilobytes while it idles.
When code needs a real Linux machine (a subprocess, a native package, a
writable filesystem, more memory or CPU than an isolate may have), it
**bursts** to a microVM from the existing [`sandbox`](../../docs/task-runtime-sandbox.md)
runtime, and only then.

The plan spends most of its length on the isolate tier: the engine, the
process and thread model, memory, the start path, how an isolate gives up
its resources while it awaits, the host API, and the security model, each
with the options considered and scored (§6). The VM tier reuses what
exists, and the plan says only what burst needs from it (§6.12). The
orchestrator is covered in one section (§8), because isolates ride
[session mode](../../docs/session-mode.md) rather than adding a placement
model.

Deliberately deferred, and named in §4: a V8 engine (Phase 7, behind a
measured trigger), moving a running isolate's continuation into a VM
(Phase 8, research), envelopes shared by many small jobs, isolates in the
managers' session index, wasm threads, and GPU access from either tier.

Relationship to other plans: this executes, and replaces the shape of,
[sandbox-scaling-plan.md](sandbox-scaling-plan.md) §6.10's Phase 9 ("Tier
2, many sessions per VM", a seam with no runtime behind it), and it is the
plan the open TODO item "A **WASM** runtime" asks for. It keeps
[mesh-interfaces-plan_calls.md entry 22](../calls/mesh-interfaces-plan_calls.md#22-the-plugin-engine-is-wasmi-an-interpreter-not-wasmtime)
(network plugins stay on wasmi) and explains in its own calls entry 3 why
the isolate host is the case that entry said would overturn it.

---

## The plan in one page

- **What runs.** A WebAssembly component, compiled ahead of time. JS and
  Python run as *images*: their interpreters compiled to Wasm and
  pre-initialized, so a JS isolate starts from a snapshot of a booted
  engine with the user's script already loaded (§6.2, §6.5).
- **Where it runs.** In `legion-isolate`, a separate binary the worker
  starts as one host process per **cordon** (a trust partition, by default
  one per namespace). One host holds thousands to a hundred thousand
  isolates from many tenants, spread over one shard per core, each shard a
  single-threaded event loop (§6.3).
- **How it starts.** A slot from wasmtime's pooling allocator, the
  module's memory image mapped copy-on-write, no compile, no syscall on
  the hot path: tens of microseconds (§6.5).
- **How it waits.** The host drives each guest's event loop. A guest runs
  a turn to completion, returns to the host when it awaits, and holds no
  stack and no thread until its I/O completes. Long waits page its memory
  out; longer ones hibernate it to disk and free its slot (§6.6, §6.7).
- **What it can touch.** A small host API: an HTTP handler, outbound HTTP
  through the node's egress datapath (policy, upstreams, and credential
  injection unchanged), a scratch filesystem in memory, coarse clocks,
  logs, tenant-scoped state, and the VM handle. The host process itself
  has no network and no filesystem beyond its compiled-module cache
  (§6.9, §6.10).
- **How it bursts.** `vm.acquire()` hands the isolate a microVM session
  of the task's burst image on the same node, which it drives over exec,
  files, and HTTP. A handler can also opt in to being replayed in a VM when
  it hits a limit or an unsupported call before it has done any egress
  (§6.12).
- **What the control plane sees.** Envelopes, as for sessions, and
  per-task aggregate counters on the heartbeat. No raft entry, no
  evaluation, and no index row per isolate or per invocation (§8).

---

## 1. Problem statement

Legion's lightest sandbox today is a microVM. It is a good one: about
0.18 to 0.23 s from create to a guest that takes exec, 150 to 200 boots a
second on a 96-vCPU node, and nothing written to raft for a session
([sandboxes at scale](../../docs/sandboxes-at-scale.md#measured-on-aws)).
For a whole class of workloads it is still two to four orders of magnitude
too heavy.

1. **Running a snippet costs a machine.** An agent platform that executes
   model-written code ("parse this CSV", "call this API and reshape the
   JSON", "evaluate this expression") runs a few milliseconds of
   interpreter per call. Today each call boots a guest kernel and an init,
   reserves at least 64 MiB of guest memory (Apple `container`'s floor is
   200 MiB), and pays a 140 ms `ready` stage before the first line runs.
   The platform either pays that per call or keeps warm VMs per user and
   pays for the idle memory.
2. **Density stops at the guest kernel.** A 128 MiB sandbox is 8 per GiB
   before any sharing. The projections top out around 120,000 running
   sandboxes on 100 bare-metal 192 GiB nodes. A platform with millions of
   mostly idle users, each wanting a live, addressable execution context,
   cannot hold them resident as VMs, and suspending them to disk costs a
   restore of hundreds of milliseconds on wake.
3. **Waiting is not free.** A VM blocked on an LLM API call for 20
   seconds holds its whole memory and its vCPU thread. Agent workloads are
   dominated by exactly that wait.
4. **There is no in-between.** The TODO list's "A **WASM** runtime" item
   names the embedding layer (`src/wasm/`) and stops at the open
   questions: an interpreter or a compiler, the CPU budget, filesystem,
   networking. Sandbox-scaling Phase 9 reserves a seam for "many sessions
   per VM" and ships no runtime behind it. A job author who wants
   something lighter than a VM today has `exec`, which is not a sandbox.

What users want is narrow to say: *run untrusted code in under a
millisecond, keep hundreds of thousands of them alive per host, let them
wait for free, and give them a real machine the moment they actually need
one, without changing how they call it.*

## 2. Prior art

(Filled in below the persona section; see §2 after §3. This placeholder is
removed when the section lands.)

## 3. Jobs to be done, by persona

**Agent platform builder** (the primary persona: runs a product that
executes code on behalf of its own users or of a model)
- When a model writes a snippet to answer a question, I want it to run
  somewhere safe in about the time the model took to emit the last token,
  so tool calls feel instant and I do not pay for a machine per call.
- When one of my millions of users comes back after an hour, I want their
  execution context (variables, open connections, the half-finished task)
  to still be there without my having paid to keep it in memory, so I can
  offer "always on" without "always billed".
- When a snippet turns out to need `pip install`, a compiler, or a binary,
  I want it to get a real Linux machine automatically, with the same
  network rules and secrets, so I do not have to predict which tier every
  piece of code needs.
- When model-written code is hostile, I want it unable to read another
  user's data or my API keys, so one prompt injection is not a breach.

**Job author / app developer**
- When I write a small HTTP handler or webhook, I want to deploy it and
  have it answer from every node within a millisecond of a cold start, so
  I can scale it to zero without a cold-start tax.
- When my handler calls a paid API, I want the key injected on the way
  out, as it is for my containers today, so the key is never in my code
  or my isolate's memory.
- When my code fails, I want to know whether it hit a limit, called
  something isolates cannot do, or crashed, in one line, so I know whether
  to fix code, raise a limit, or let it burst.

**Cluster operator**
- When I give the cluster to isolates, I want to know how many fit on a
  node and see the host processes as ordinary, bounded things I can drain,
  so isolates do not become a second, invisible fleet.
- When an engine CVE is announced, I want to turn the isolate tier off
  (everything bursts or fails cleanly) in one command and turn it back on
  after upgrading, so the response is minutes, not a migration.

**Security / platform admin**
- When tenants of different trust share a node, I want to choose which of
  them may share a process, so a free-tier tenant's code never runs beside
  a tenant whose isolates handle credentials.
- When I approve the isolate tier, I want the host process confined so
  that escaping the engine still leaves an attacker with no network, no
  filesystem, and no other cordon, so the engine is not the only wall.

**Autonomous agent** (driving Legion directly)
- When I need to run code as a step of a task, I want one call that
  returns stdout, stderr, the result, which tier ran it, and why it burst,
  as structured data, so I can act on the outcome without parsing text.
- When I retry after a timeout, I want the retry not to run my code twice,
  so non-idempotent snippets are safe to retry.

*Not in scope:* the **on-call responder** gets no new surface beyond
existing allocation, log, and metrics views (the cordon host is an
allocation-like process with logs and metrics; isolates are aggregated,
not individually inspectable in the UI, in this plan).

### User scenarios

1. **The code-interpreter tool.** An agent platform submits one job,
   `code-runner`, with a Python image and a burst image of
   `python:3.13-slim`. Its backend calls `RunIsolate` with each snippet.
   99% of calls finish in the isolate in 2 to 20 ms. A snippet that imports
   `torch` is routed straight to a VM by the pre-flight check; one that
   shells out to `ffmpeg` gets `unsupported`, and because it has not made
   an outbound request yet, it is replayed in a VM. The reply says
   `tier = "vm"` and why. The next run of the same code hash goes straight
   to a VM.
2. **A million resident agents.** Each of the platform's users has an
   isolate session (`lifetime = "session"`) holding their agent loop's
   state. While the loop awaits the model, the isolate holds no stack and
   no thread. After 10 seconds idle it is paged out; after 2 minutes it is
   hibernated to the node's disk and its slot is freed. A message for the
   user rehydrates it in about a millisecond on the node its id names.
3. **Scale-to-zero webhooks.** A job author ships a Rust component as an
   HTTP handler on every node of a pool. Each request gets a fresh
   instance (`lifetime = "request"`), so no state leaks between requests,
   and the first request after an idle week costs the same as the
   millionth.

## 4. Goals and non-goals

### Goals

- **G1: Start in microseconds.** A precompiled component instantiates in
  ≤ 20 µs p50 and ≤ 100 µs p99 on the host it runs on. A JS image with
  the author's script pre-initialized is running in ≤ 300 µs p50. A
  dynamic JS snippet of 10 KB is running in ≤ 1 ms p50. A Python image
  with a small snippet is running in ≤ 5 ms p50.
- **G2: Maximum density.** At least 4,000 small (Rust/Go/C) isolates and
  500 JS isolates resident per GiB of host memory while idle, and at least
  10^5 resident isolates per 64 GiB host, with ≥ 10^6 hibernated isolates
  per node bounded by disk.
- **G3: Waiting is free.** An isolate awaiting I/O holds no OS thread and,
  for guests on the stackless async ABI, no stack. Switching between
  isolates on a shard costs ≤ 1 µs.
- **G4: Many tenants per process,** partitioned by an operator-chosen
  trust boundary, with per-tenant CPU fairness and hard per-isolate limits.
- **G5: Burst only when needed.** A burst reaches a VM in the existing
  sandbox's boot time (≤ 250 ms p50) and in ≤ 50 ms once warm-snapshot
  restores exist. Code that never needs a VM never touches one.
- **G6: Defense in depth.** An engine escape lands in a process with no
  network, no writable filesystem, no other cordon's memory, and no way to
  generate new code.
- **G7: Nothing per isolate on raft.** Isolate starts, invocations, and
  hibernations write nothing to the replicated store and nothing to the
  managers' session index.
- **G8: Agent-ready.** One structured call to run code, with idempotency,
  a dry-run (`validate`), an MCP tool behind `--allow-write`, and
  catalog entries.

All numbers above are targets that Phase 0 replaces with measurements. A
target Phase 0 shows to be wrong is rewritten here with a calls entry.

### Non-goals

- **Running arbitrary Linux binaries in an isolate.** That is what burst
  is for. Isolates run Wasm.
- **POSIX compatibility in the isolate tier.** No `fork`, no processes, no
  raw sockets, no persistent filesystem. WASIX-style POSIX emulation is
  rejected (§6.9).
- **A JIT JavaScript engine in Phase 1 to 6.** JS runs on an interpreter
  with ahead-of-time partial evaluation. V8 is Phase 7, behind a trigger.
- **Live migration of a running isolate into a VM.** Phase 8, research.
- **Envelopes shared by thousands of small jobs** (one per FaaS customer).
  This plan's density comes from one job hosting many tenants. A shared
  pool envelope is an open question (§14).
- **Isolate sessions in the managers' index, and per-isolate UI.** Nodes
  answer for their own isolates; the UI shows aggregates.
- **Wasm threads and shared memory.** They are a high-resolution timer and
  a scheduling hazard (§6.10).
- **GPU access** from either tier.
- **Determinism / replayable execution** beyond the replay-on-burst rule.

## 5. Background: what exists today

### 5.1 The WASM embedding layer

`src/wasm/` (`src/wasm/mod.rs:1-37`) is the shared layer the network
plugins use: one engine with fuel metering (`src/wasm/engine.rs:9-16`), a
memory cap that remembers a refusal (`src/wasm/engine.rs:21-71`), failure
descriptions (`describe_failure`, `src/wasm/engine.rs:78-97`), a WASI
preview1 shim with arguments, environment, clocks, randomness, and
stdout/stderr (`src/wasm/wasi.rs`), and module fetching by URL and
`sha256:` through the artifact stager into the node content store
(`src/wasm/module.rs`). It embeds **wasmi 2.0**, an interpreter
(`Cargo.toml:128`), chosen for binary size: wasmi adds 1.8 MB, wasmtime
with Cranelift about 10 MB, to a ~26 MB worker
([mesh-interfaces calls entry 22](../calls/mesh-interfaces-plan_calls.md#22-the-plugin-engine-is-wasmi-an-interpreter-not-wasmtime)).

The proxy-wasm host keeps a small pool of instances per (plugin revision,
scope, config), each behind a mutex for the length of a callback, picked
"first idle, else round-robin" (`src/worker/netd/plugins/mod.rs:618-699`).
That is the right shape for a few plugins per node and the wrong one for
10^5 tenants: one instance per slot, synchronous callbacks, fuel per call,
and no notion of an instance waiting.

### 5.2 Session mode, envelopes, and edge starts

Session mode ([docs/session-mode.md](../../docs/session-mode.md)) already
runs "very large numbers of short-lived, on-demand units, such as an
agent's sandbox, a tool execution, a CI runner, or a function invocation"
with no write to raft. The pieces this plan rides:

- **Envelopes.** A session task generates `<task>-envelope` on the
  built-in `envelope` runtime (`src/task_plugins/envelope.rs:1-7`), one
  allocation per eligible node, which reserves the room. "The allocation is
  the reservation, so there is nothing to start." Elastic envelopes
  (`stateful.envelope.max`, `shrinkAfter`) grow and shrink per node.
- **The session router and edge starts.** A manager routes a start to a
  node with room over `PostWork`; at the edge a caller holding a session
  ticket calls `Worker.StartSession` with no manager on the path, and the
  node verifies the ticket offline.
- **Ids that name their node.** An unnamed session's id carries an
  8-character tag of its node (`workspace-q4n7c2rd.7k2qd9x4…`), so routing
  a later request needs no lookup.
- **The launch pool.** `edgeBoot.launchConcurrency` slots bound concurrent
  starts per node, refusing with retryable `RESOURCE_EXHAUSTED` after
  `edgeBoot.queueTimeout` (`src/worker/launch_pool.rs:1-12`). A slot is
  sized for a VM boot; an isolate start is four orders of magnitude
  cheaper and must not take one (§8).
- **Sleeping and moving sessions.** `suspend.durability = "local" |
  "store"`, `idle.after`, `keepFor`, manifests with generations in the
  content store (`src/worker/task_manager/session_sleep.rs:1-15`,
  `session_store.rs:1-15`). Isolate hibernation reuses this vocabulary and,
  for `store`, the manifest format.
- **What it costs to report.** The session index is about 200 bytes per
  running session per manager ([scaling plan §7.1](sandbox-scaling-plan.md#71-throughput-and-counts)).
  At 10^5 isolates per node across 100 nodes that is 2 GB per manager,
  which is why isolates are not reported per unit (§8).

### 5.3 The sandbox runtime (the burst target)

The `sandbox` runtime (`src/task_plugins/sandbox.rs`, 4,511 lines;
`src/task_plugins/microvm.rs`, 2,660 lines) boots an OCI image in a
cloud-hypervisor or Firecracker microVM. What burst reuses unchanged:

- **The root disk cache:** one base disk per image per node, reflinked per
  sandbox in about 10 ms on XFS ([sandbox doc](../../docs/task-runtime-sandbox.md#the-root-disk-cache)).
- **The guest agent's exec listener** on vsock port 1100, so the isolate
  can run commands in its VM ([sandbox-exec-plan.md](sandbox-exec-plan.md)).
- **Gated networking with credential injection**, so a burst VM reaches
  exactly the upstreams its job declares with the same injected keys
  ([network handlers](../../docs/network-handlers.md#credential-injection)).
- **Session mode for sandboxes**, idle suspend, and sleeping on the node.
- **Snapshots, forks, and create-from-snapshot** for keyed instances.
  Sessions cannot start from a snapshot yet
  ([session doc](../../docs/session-mode.md): "Not available for sessions:
  `--from-snapshot`"). A warm burst therefore needs
  [scaling plan Phase 8](sandbox-scaling-plan.md) ("a warm base checkpoint
  per envelope"); until then a burst is a cold boot (§6.12).

### 5.4 The datapath the isolate's I/O goes through

- **Gated egress:** an empty network namespace per allocation with the
  egress proxy, resolver, upstream listeners, and ingress listeners bound
  inside it ([gated mode](../../docs/network-handlers.md#gated-mode)).
  Per-isolate namespaces are impossible at 10^5 per node, so the host
  process carries each isolate's identity on its requests instead (§6.9).
- **PROXY protocol v2 with Legion TLVs** (`src/worker/netd/proxyproto.rs:1-19`)
  already names a flow's workload, allocation, and labels to something that
  owns bytes. The isolate host's requests to the worker carry the same
  TLVs, so policy, upstreams, and credential injection resolve per isolate.
- **Ingress** listeners and the gateway's wake-on-request
  ([gateway](../../docs/gateway.md)) are the HTTP entry for request and
  session isolates.

### 5.5 Confinement and accounting the host process reuses

- A seccomp denylist for containers (`src/task_plugins/seccomp.rs:1-25`),
  user namespaces (`src/task_plugins/userns.rs`), and cgroup v2 resource
  enforcement with oversubscription and a named memory-pressure response
  (`src/worker/pressure.rs:1-12`,
  [resource enforcement](../../docs/resource-enforcement.md)).
- Idle detection (`src/worker/idle.rs:1-12`) and the per-node memory
  pressure steps, which learn one new step: hibernate isolates (§6.7).
- Namespace policies already restrict which runtimes a namespace may use
  (`runtimes = ["container", "sandbox"]`,
  [namespaces](../../docs/namespaces.md)), which is how an admin keeps a
  namespace off the isolate tier, or on it only.
- A shipped helper binary has a precedent: `legion-guest`, found beside
  the worker or through `LEGION_GUEST_BINARY`
  ([sandbox doc](../../docs/task-runtime-sandbox.md#the-guest-agent)).
  `legion-isolate` is found the same way.
