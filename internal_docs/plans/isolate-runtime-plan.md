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
(network plugins stay on wasmi) and explains in its own calls entry 4 why
the isolate host is the case that entry said would overturn it, and why the
worker binary still does not grow.

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
  module's memory image mapped copy-on-write, imports resolved ahead of
  time, no compile and no file I/O: tens of microseconds (§6.5).
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

Researched 2026-10-28. Vendor numbers are each vendor's own, under its own
hardware and definition of "started"; read them as orders of magnitude.

### Isolate platforms in production

- **Cloudflare Workers** ([2018](https://blog.cloudflare.com/cloud-computing-without-containers/),
  [security model](https://developers.cloudflare.com/workers/reference/security-model/),
  [Dynamic Workers, 2026-03-24](https://blog.cloudflare.com/dynamic-workers/)).
  V8 isolates, "hundreds or thousands" per process (Cloudflare now says
  "tens of thousands of tenants" share one process), about 5 ms and a few
  MB each, 128 MB memory per isolate, CPU enforced by a CPU-time signal and
  `TerminateExecution`, LRU eviction under a global memory cap. One isolate
  serves many concurrent requests; a thread runs one isolate at a time and
  releases it between turns. The runtime process sits inside namespaces
  and seccomp with no filesystem and Unix sockets only, egress through a
  proxy, and code fetched by a supervisor; tenants are split by trust into
  **cordons** ("a free-plan customer will not be scheduled in the same
  process as an enterprise customer"); suspicious Workers are moved into
  their own process by performance-counter detection; and since September
  2025 every isolate's heap has its own Intel MPK key
  ([2025-09-25](https://blog.cloudflare.com/safe-in-the-sandbox-security-hardening-for-cloudflare-workers/)).
  Dynamic Workers (`load()`) create an isolate per call for AI-generated
  code, with no network unless granted. In August 2026 researchers leaked a
  JWT across tenants in production at up to 12 bit/s with a remote timer
  over a WebSocket, defeating frozen local clocks; the process-isolation
  detector missed it because it judged only finished invocations
  ([2026-08-19](https://blog.cloudflare.com/revisiting-spectre-attacks-on-workers)).
  - **Take:** cordons by trust; the confined runtime process with no
    filesystem, no network, and egress through a proxy; an LRU cap with
    eviction; capabilities granted, not ambient; detection on *running*
    work, not finished work.
  - **Reject:** V8 as the engine (§6.1), and one long-lived isolate per
    script as the only lifetime.
- **Fastly Compute** ([Wasmtime 1.0](https://bytecodealliance.org/articles/wasmtime-1-0-fast-safe-and-production-ready),
  [JS on Compute](https://www.fastly.com/blog/compute-edge-now-supports-javascript)).
  Wasmtime with the pooling allocator and copy-on-write memory images: a
  fresh instance per request in about 5 µs (SpiderMonkey.wasm went from 2
  ms to 5 µs). JS runs as SpiderMonkey compiled to Wasm (StarlingMonkey),
  which Fastly calls an outer boundary around the JS engine; reuse is
  opt-in, because it "results in a larger blast radius". weval AOT gives a
  2.77× geomean over the interpreter
  ([2024-11-11](https://www.fastly.com/blog/speeding-up-javascript-on-compute)).
  - **Take:** the whole engine stack: wasmtime, pooling, CoW, instance per
    request by default, JS inside Wasm.
- **Shopify Functions** ([Javy](https://shopify.engineering/javascript-in-webassembly-for-shopify-functions)).
  QuickJS compiled to Wasm, about 3× slower than equivalent Rust Wasm,
  with hard limits (256 kB binary, 10 MB linear memory, 11M instructions
  for a cart of up to 200 lines). Chosen because a JIT is impossible inside
  Wasm and QuickJS is plain C.
  - **Take:** QuickJS as the small JS image candidate; instruction and
    memory limits as first-class, published numbers.
- **Fermyon Spin / Akamai** ([SpinKube](https://www.akamai.com/blog/developers/2025/jul/spinkube-k3s)).
  wasmtime components, "0.5 ms cold start" and "thousands of apps per
  node" (vendor claims, no method).
  - **Take:** WASI components as the packaging format.
- **Deno Deploy** ([2023](https://deno.com/blog/subhosting-security-run-untrusted-code),
  [2024](https://deno.com/blog/build-secure-performant-cloud-platform)).
  Each isolate in its own process and namespace, with seccomp and cgroups,
  and runners are "pools of V8 isolates in a virtual machine".
  - **Take:** the VM around a pool of isolates, as Legion's optional
    `cordonBoundary = "vm"`.
- **Azure Front Door edge actions** (public preview 2026-07-14) run
  JavaScript in Hyperlight micro-VMs with 16 KB of code and 10 ms of
  execution ([docs](https://learn.microsoft.com/azure/frontdoor/edge-actions)).
  - **Note:** the first production use of a hardware-isolated middle tier.

### Kubernetes and Nomad

- **Kubernetes** runs Wasm through containerd shims
  ([runwasi](https://github.com/containerd/runwasi), SpinKube) selected by
  a `RuntimeClass`. Each app is still a pod with its own shim process,
  sandbox, and API object, so density is the pod's density and a cold
  start includes the kubelet.
  - **Reject:** a unit per app in the control plane. Legion's isolates
    are not API objects at all (§8).
- **Nomad** has no first-class Wasm driver; community task drivers run a
  Wasm runtime per task, a process per allocation, each a raft object.
  - **Reject:** the same, for the same reason.

### Research systems

- **Faasm** ([ATC '20](https://arxiv.org/abs/2002.09344)). Wasm
  "Faaslets" with snapshot-restored "Proto-Faaslets": 0.5 ms and 90 KB
  each, more than 100,000 per host, against Docker's 2.8 s and ~8,000.
  A snapshotted CPython restored in 0.9 ms.
  - **Take:** pre-initialized snapshots as the start path, and the
    density they buy.
- **Sledge** ([Middleware '20](https://faculty.cs.gwu.edu/gparmer/publications/middleware20sledge.pdf))
  and **Lucet** ([Fastly 2019](https://www.fastly.com/blog/lucet-performance-and-lifecycle)):
  30 µs starts; Lucet's 23 µs teardown and 0.5 µs context switch.
  - **Take:** the switch-cost target.
- **Dandelion** ([SOSP '25](https://arxiv.org/html/2505.01603v2)). Pure
  compute functions that make no syscalls, with I/O only through trusted
  communication functions; sandbox creation from 89 µs (CHERI) to 889 µs
  (KVM); 96% less committed memory than Firecracker with Knative on the
  Azure Functions trace. It also measured Hyperlight Wasm at 9.1 ms cold.
  - **Take:** compute isolated from I/O, with I/O done by a trusted party
    (Legion's worker).
- **ColorGuard** ([ASPLOS '25](https://zyedidia.github.io/papers/seguecg.pdf)).
  MPK striping in wasmtime's pooling allocator: 14,582 → 218,716 slots of
  408 MB per process, switch cost 30 → 52 ns, up to 29% more throughput
  than 15 processes. Upstream, experimental.
  - **Take:** in Phase 6 (§6.4).
- **weval** ([PLDI '25](https://cfallin.org/pubs/pldi2025_weval.pdf)).
  Partial evaluation of an interpreter into compiled code: 2.17× on Octane
  over SpiderMonkey's interpreter with inline caches.
  - **Take:** for the `js` image.
- **SEUSS, Catalyzer, SOCK, Junction, TrEnv.** Unikernel snapshots
  (> 50,000 cached instances), checkpoint-restore with `sfork` (< 1 ms),
  zygotes with import caches (45× over Docker), kernel bypass (19 to 62×
  denser), and memory templates (< 10 ms containers).
  - **Take:** the consensus that starts come from restoring prepared
    state, never from booting faster.

### VM fast start, and how others escalate

- **Nobody moves a running isolate's memory into a VM.** Every production
  escalation is explicit, and state crosses tiers through a filesystem or
  object store. Cloudflare's Project Think climbs Workspace → Dynamic
  Worker → npm → browser → Sandbox as the agent "escalates as needed",
  with the Sandbox's files "synced bidirectionally with the Workspace"
  ([2026-04-15](https://blog.cloudflare.com/project-think/)). Containers
  start when a Durable Object calls `ctx.container.start()`. Modal picks
  gVisor or a VM per sandbox (`runtime = "vm"`); Vercel and Deno start a
  sandbox from an SDK call. Cloudflare's Sandbox SDK and Deno Sandbox
  both keep credentials outside the sandbox and add them on egress.
  - **Take:** an explicit, application-visible escalation (`vm.acquire`),
    files as the handoff, credentials added on egress. Legion adds
    automatic replay for code that has not yet done egress (§6.12).
  - **Reject (defer):** continuation migration, which nobody ships.
- **Prepared VMs, not faster boots.** Cloudflare cut median container
  start from 4.0 s to 648 ms by restoring "a prepared virtual machine that
  isn't yet assigned" ([2026-09-30](https://blog.cloudflare.com/faster-agent-sandboxes/)).
  Unikraft holds a million scaled-to-zero VMs on one 48-core box and
  resumes in under 10 ms from compressed differential snapshots on NVMe
  ([2026](https://unikraft.com/blog/1m-vms-single-box)). Firecracker
  restores in as little as 4 ms ([Brooker](https://brooker.co.za/blog/2022/11/29/snapstart/)),
  and the Zeroboot prototype forks a Firecracker snapshot into a new KVM
  VM in 0.79 ms p50 at about 265 KB private each (one vCPU, no network,
  [repo](https://github.com/zerobootdev/zeroboot)). Lambda SnapStart
  records and preloads each function's working set.
  - **Take:** a per-node pool of prepared, unassigned burst VMs restored
    from a base snapshot, with the guest agent's existing resume handler
    refreshing identity, RNG, and clock on assignment (§6.12).
- **Hyperlight** ([2024](https://opensource.microsoft.com/blog/2024/11/07/introducing-hyperlight-virtual-machine-based-security-for-functions-at-scale/),
  [repo](https://github.com/hyperlight-dev/hyperlight)). Micro-VMs of one
  vCPU and a memory slice with no devices: 1 to 2 ms create (vendor),
  9.1 ms measured for Hyperlight Wasm by Dandelion, 45 to 93 µs snapshot
  loads, embeddable from Rust, pre-1.0, Hyperlight Wasm "experimental".
  - **Watch:** a candidate cordon boundary (§14 Q6), not the isolate tier.

### What the prior art changes in this plan

1. Isolates are not the only wall anywhere they run in production, so the
   host process is confined and holds no secrets (§6.10).
2. The engine Legion should embed is the one Fastly runs, not the one
   Cloudflare runs: Fastly's stack fits a Rust codebase, starts three
   orders of magnitude faster, and keeps JS engine bugs inside Wasm (§6.1).
3. Escalation is explicit and file-based everywhere; automatic replay is
   Legion's addition, kept narrow (§6.12).
4. A warm VM tier comes from prepared VMs, which Legion's snapshots and
   guest agent already nearly provide (§6.12).

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

## 6. The decision space

Each subsection names the options that were genuinely on the table, scores
them against the three things the isolate tier exists for (start time,
density, and waiting for free) plus security and fit with Legion, and
picks one. The judgement calls doc records the reasoning behind the
biggest picks.

### 6.1 The engine

| Option | Start | Idle cost | Tenants per process | Waiting | Languages | Escape record | Binary | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **wasmtime, pooling allocator, CoW images** | 5 to 30 µs | its private pages: KBs for a small module, ~1 MB-class for a JS heap | 16k to 32k slots; ~218k with MPK | stackless callback ABI holds no stack; fibers otherwise | anything that compiles to Wasm; JS and Python as interpreters inside | 5 critical advisories in 2026, mostly compiler backends (aarch64, Winch), one in the default config on every architecture; none in 2024 and 2025 | runtime-only +1.2 to 2.1 MB; with Cranelift ~19 MB | **Take** |
| V8 isolates (`v8` crate, `deno_core`, or workerd) | 0.7 to 5 ms from a snapshot | a few MB | hundreds to thousands; one 4 GB shared cage by default, multi-cage ("IsolateGroups") unsupported upstream and unreleased in the Rust crate; a 2022 report of a 256 to 512 isolate cap with the shared cage, not confirmed fixed | native: promises hold no stack | JS, TS, Wasm | V8 bugs start about half of Chrome's 7 to 10 in-the-wild 0-days a year; workerd's README says it "is not a hardened sandbox" | +22 to 42 MB, roughly doubling the binary | **Defer** to Phase 7 behind a trigger |
| wasmi (today's plugin engine) | no compile | as wasmtime | as wasmtime | `call_resumable` suspends at host calls | as wasmtime | tiny interpreter | 1.8 MB | Keep for plugins; **reject** here: ~10× slower than Cranelift, in Pulley's band |
| WAMR, WasmEdge, Wasmer | fast AOT (1.6 to 2.1× native on libsodium vs wasmtime's 2.4×) | small | fine | weaker async and component story | as wasmtime | WAMR and WasmEdge are C/C++ runtimes inside the multi-tenant process | small | **Reject** |
| Hyperlight micro-VMs | 1 to 2 ms (vendor), 9.1 ms measured; 45 to 93 µs snapshot loads | MBs (hundreds per GiB) | one VM per tenant, many per process | a host call is a VM exit (two on nested virtualization) | Wasm or JS inside the VM | hardware boundary; pre-1.0 | needs KVM/MSHV | **Reject** as the isolate tier; watch as a cordon boundary |
| A process per tenant from a zygote (seccomp, Landlock, user namespaces) | 0.5 to 2 ms (Dandelion 486 µs; 2.3 ms fork+exec nested, namespaces +~1.5 ms) | MBs of kernel and page-table state | one | the kernel's | any native code | kernel attack surface | none | **Reject**: fails "many tenants per process"; the VM tier does it better |
| gVisor | ~190 ms | ~24 MB | one sandbox | the Sentry's | any | strong | separate | Reject for this tier (a VM-tier alternative) |
| Native QuickJS in the host | < 300 µs | hundreds of KB | many | engine-level | JS only | a C engine in the shared process: any bug owns every tenant | small | **Reject** |

**Why wasmtime and not V8.** V8 is the only engine with a production record
of thousands of tenants per process, and Cloudflare's design is the
reference for operating one. But on the axes this plan optimizes, Wasm
wins each:

- **Start.** 5 µs to a fresh instance against 0.7 to 5 ms: an
  instance per request is affordable, so the default lifetime can be the
  safe one.
- **Density.** An idle instance costs only what it wrote; a V8 isolate's
  floor is megabytes before user code. For Rust, Go, and C guests the gap
  is one to two orders of magnitude.
- **Containment.** JS and Python interpreters run *inside* Wasm, so an
  engine bug, the class behind most V8 exploits, corrupts one isolate's
  linear memory instead of the process. The trusted computing base in the
  shared process is wasmtime's runtime and Cranelift's output, both in
  Rust or verified-in-progress, not a JIT for a dynamic language.
- **Fit.** wasmtime is a Rust crate with a runtime-only build of about
  2 MB; V8 would roughly double Legion's binary and bring a C++ toolchain.
  Upstream does not support lifting V8's 4 GB shared cage.

**What it costs: JS throughput.** StarlingMonkey in Wasm scores about 1/75
of V8's JIT on Octane before weval, which narrows that by 2 to 3×; QuickJS
natively is about 1/30. For agent snippets, glue code, and request
handlers that spend their time waiting on I/O, that is the right trade.
For CPU-bound JS it is not, and the answer is the burst tier, not a second
engine: a burst VM runs Node with V8's JIT behind a hardware boundary
(`burst.on = ["limits"]`). Phase 7 adds V8 as a second engine only if
Phase 3's telemetry shows the trigger in calls entry 1.

### 6.2 Languages and images

An **image** is a pre-initialized component that Legion ships or the
author builds; a tenant's code is either compiled into a component or
handed to an image as source.

| Language | How | Start | Notes | Verdict |
| --- | --- | --- | --- | --- |
| **Rust** | components via `wit-bindgen`, including async on the callback ABI | 5 to 30 µs | the best-supported guest; smallest footprint | First class |
| **Go** | `componentize-go` (Go 1.25.5+); TinyGo `wasip2` | µs | mainline `GOOS=wasip2` not shipped | Supported |
| **C, C++, .NET, MoonBit** | wasi-sdk; componentize-dotnet (NativeAOT-LLVM) | µs | | Supported |
| **JS: StarlingMonkey** (SpiderMonkey in Wasm, weval AOT) | the `js` image; ComponentizeJS for an author's own component | 5 µs instantiate, 0.36 ms to hello world | full modern JS and Web APIs (`fetch`, streams, crypto); runs Fastly's and Spin's JS; ~8 MB image shared copy-on-write | **Default `js`** |
| JS: QuickJS / quickjs-ng (Javy) | a `js-small` image | < 300 µs lifecycle | ~1 MB, reference counting (smaller private heaps), ~3× slower than Rust Wasm; Shopify's choice | **Candidate**, if Phase 0 shows a several-times smaller idle footprint |
| JS: Boa, Hermes, MicroQuickJS | | | Boa: 95.6% test262 but the slowest on Octane; Hermes: no server embedding story; MicroQuickJS: ES5 subset | Reject |
| **Python: CPython for WASI** (componentize-py, pre-initialized) | the `python` image | 1 to 5 ms per run after pre-initialization (one embedder's report); < 1 ms instantiate from `InstancePre` (maintainer's estimate) | pure-Python packages; native extensions only where a Wasm build exists | **Default `python`** |
| Python: Pyodide | | ~1 s cold even with Cloudflare's memory snapshot | built for Emscripten and a JS host, not WASI | Reject |
| Python: MicroPython, RustPython | | ms | MicroPython is 303 KB but not CPython; RustPython is not production-ready | Reject (MicroPython a possible `python-small`) |

Three rules follow:

1. **Images are code Legion ships and pins.** Each release pins an image
   digest per name; a job can pin an older one.
2. **What an image cannot do, it says so, by name.** Python's
   `subprocess`, `ctypes`, and socket modules, and JS's `child_process` and
   `node:` builtins outside the supported set, are stubs that raise
   `IsolateUnsupported("<what>")` and report it to the host, so a snippet
   that needs a machine fails in a way burst can act on (§6.12).
3. **No package installer in an isolate.** Packages are in an image or
   bundled into the author's component. `pip install` is a VM's job.

**Python is the honest weak spot.** Cloudflare's Python Workers, the most
engineered Python-on-isolates in production, start in about a second with
real packages. Legion's target (≤ 5 ms) holds only for the standard
library plus small snippets; anything heavy is routed to a VM by the
pre-flight check, and the public doc says so.

### 6.3 Process and thread model

Three questions: whether isolates run in the worker process, how tenants
map to processes, and how a process maps to cores.

| Option | Start and switch cost | Blast radius of an engine escape | Density | Verdict |
| --- | --- | --- | --- | --- |
| **A. In the worker process** | Lowest: no IPC for I/O | The worker: every allocation's control, the node's credentials, the datapath | One address space for everything | **Reject** |
| **B. One host process per job per node** | IPC per I/O | One job | Bounded by job count: a FaaS shape with 10^4 jobs is 10^4 processes | Phase 1 only (§11), as `cordon = "job"` after |
| **C. One host process per cordon** (a trust partition, default per namespace) | IPC per I/O | One cordon, which holds no secrets (§6.10) | Many tenants per process, the point of the exercise | **Take** |
| **D. One process per tenant from a zygote** | ~1 ms fork, MiBs of kernel state | One tenant | Kernel-bound, ~10^4 per node | Reject for the isolate tier (it is what the VM tier does better) |

**Option C, with the cordon as the unit of trust.** The worker starts one
`legion-isolate` host process per cordon on its node when the first
envelope that needs it lands, and stops it when the last leaves. Cordon
kinds (`config.cordon`):

- `namespace` (default): every isolate task in the namespace shares a
  host. A platform job hosting a million end-user tenants is one cordon.
- `job`: a host for this job alone, for a job whose isolates handle
  material it will not share a process with anyone over (the kernel then
  enforces its envelope too; §14 Q7).
- `shared`: one host for every namespace whose policy allows it, for
  operators running anonymous, low-trust code at maximum density. An
  operator opts in per namespace (`isolateCordons`).

A cordon may need more than one process. Without MPK, a process holds
about 16,000 to 21,000 guard-page slots (§6.4), so a cordon that needs more
runs several host replicas and the worker spreads new isolates across
them. That is the fallback the ColorGuard paper measured MPK against
(about 29% more throughput with one MPK process than with 15 processes).

**Inside a host: one shard per core, shared nothing.** Each shard is a
thread pinned to a CPU of the cordon's cpuset, running a single-threaded
async executor that owns a set of isolates (their wasmtime `Store`s). An
isolate runs only on its shard, so its memory stays in that core's caches
and nothing in a turn takes a lock. New request isolates go to the less
loaded of two random shards; session isolates stay on their shard and
move only between turns, when a rebalancer sees a shard persistently
over its share. The shard count follows the cordon's CPU share and is
resized between turns.

| Thread model | For | Against | Verdict |
| --- | --- | --- | --- |
| **Thread per core, shared nothing** (Seastar, Glommio, Redpanda's wasm) | No locks in a turn, cache-warm isolates, a predictable switch | Imbalance needs an explicit rebalancer | **Take** |
| Work-stealing (tokio multi-thread) | Balances itself | Isolates migrate between cores mid-life, `Store` access needs synchronization, and the slice accounting has to follow the task | Reject for guest execution; the host's I/O to the worker may still use it |
| A thread per isolate | Simple, preemptive | 10^5 threads; defeats "waiting is free" | Reject |

Redpanda's engineers ran wasmtime on Seastar's reactor threads with 128
KiB fiber stacks and reported three times the throughput of their first
design, which is the closest public evidence for this shape
([rockwotj](https://rockwotj.com/blog/async-wasm-in-seastar)).

**The hard rule this implies:** nothing on a shard thread blocks. Host
functions are async or complete in microseconds. Compilation never runs on
a shard (it is a different process, §6.5), and the first touch of a newly
mapped compiled module is prefetched off the shard before the module is
marked ready.

### 6.4 Memory layout

Each isolate's linear memory is a slot in wasmtime's pooling allocator.
The decision is how slots are laid out in the host's virtual address
space, because on x86-64 that, not RAM, is what caps isolates per process.

| Layout | Slots per process (x86-64, 47-bit) | Run-time cost | Spectre on heap accesses | Maturity | Verdict |
| --- | --- | --- | --- | --- | --- |
| **Guard pages** (4 GiB reservation + guard per slot; wasmtime's default) | ~16,000 to 32,000 ([docs](https://docs.wasmtime.dev/api/wasmtime/struct.PoolingAllocationConfig.html), ColorGuard) | None: bounds checks elided | Safe by construction: a speculative index stays inside the slot's own reservation | Default, most tested | **Take as the default** |
| **MPK striping / ColorGuard** (up to 15 slots share a reservation, each a different protection key) | ~15× more: 14,582 → 218,716 slots of 408 MB measured ([ColorGuard, ASPLOS '25](https://zyedidia.github.io/papers/seguecg.pdf)) | A switch costs 30 → 52 ns | MPK also blocks speculative access to other colours (per the paper) | Upstream but "experimental", off by default, x86 Linux only | **Take in Phase 6**, behind its own soak, default-on per cordon only after it |
| **Small reservations + explicit bounds checks** | ~10^6 | ~1.5× slower; 1.6 to 1.8× with Spectre guards | Needs the Spectre `cmov` guards | Tested, but the zero-guard corner has had a leak ([CVE-2026-34988](https://github.com/bytecodealliance/wasmtime/security/advisories/GHSA-6wgr-89rj-399p)) | **Reject as a default**; an operator density mode for `shared` cordons only |
| **5-level paging** (57-bit addresses, 128 PiB) | ~10^6 with guard pages | None | As guard pages | Kernel opt-in per mapping; whether wasmtime's pool asks for high addresses is unverified | **Investigate in Phase 6** |
| Several host processes per cordon | Slots × processes | One more process per ~16k slots | As guard pages | Nothing new | **Take as the fallback** without MPK (ARM, older x86) |

The density-tuned layouts are the least tested paths in the engine (two of
2026's advisories live there), so the layout is a property of the cordon,
set by the operator, never a per-job knob.

**The per-isolate memory cap defaults to 128 MiB** (Cloudflare Workers'
limit), with a worker-wide ceiling of 4 GiB. 128 MiB is far below the
~550 MB at which MPK striping reaches its full 15× (8 GiB / 15), so the
default never fights the density layout.

**Slot reuse is where instantiation time goes at high rates.** The
pooling allocator's measured bottleneck at hundreds of thousands of
instantiations a second is `madvise` and the TLB-shootdown IPIs it causes
([wasmtime #8034](https://bytecodealliance.github.io/zulip-archive/stream/225357-git-wasmtime/topic/wasmtime.20.2F.20issue.20.238034.20Wasmtime.20throughput.20low.20on.20linux.20s.2E.2E.2E.html)).
The host uses the knobs the docs recommend for that: slot affinity to a
module (`max_unused_warm_slots` raised from 100), `linear_memory_keep_resident`
for the first pages of small modules (reset with `memset`, not
`madvise`), `pagemap_scan` on Linux 6.7+ to reset only resident pages,
and batched decommits. Pool sizes (`total_memories`, `total_stacks`,
`total_core_instances`, default 1,000 each) are sized from the host's
slot budget.

**Copy-on-write images** (`memory_init_cow`, on by default on Linux)
mean a module's initial memory, including a pre-initialized JS heap, is
mapped from the compiled artifact and shared by every instance until
written. An idle isolate's cost is its *private* pages: what it wrote.

### 6.5 The start path and compilation

What happens between "run this" and the guest's first instruction,
fastest first:

| Start | What it costs | When |
| --- | --- | --- |
| **Warm instance reuse** | A turn on an existing instance: < 1 µs | `lifetime = "tenant"` or `"session"`, the instance is resident |
| **Fresh instance from a cached artifact** | A pool slot, a CoW map of the memory image, `InstancePre` (imports resolved ahead): 5 to 30 µs (SpiderMonkey.wasm in 5 µs, [Fallin 2022](https://bytecodealliance.org/articles/wasmtime-10-performance); Lucet 30 µs) | `lifetime = "request"`, every call |
| **Fresh instance plus dynamic source** | The above from a pre-initialized language image, then the image parses the snippet: hundreds of µs to a few ms | `RunIsolate` with `source` |
| **Wake from page-out / hibernation** | Page faults, or a slot plus a diff read: §6.7 | A session isolate after idling |
| **Compile, then instantiate** | Cranelift: tens to hundreds of ms per MB of Wasm | First use of a module on a node, or dynamic Wasm |

**Pre-initialization makes language images fast.** Wizer, now `wasmtime
wizer` inside Wasmtime (merged October 2025), runs a module's
initialization once and snapshots the resulting memory into the module's
data segments. SpiderMonkey plus Wizer started in 0.36 ms against a JS
isolate's ~5 ms ([Clark 2021](https://bytecodealliance.org/articles/making-javascript-run-fast-on-webassembly)),
and a snapshotted CPython restored in 0.9 ms in Faasm. Legion pre-initializes
twice:

1. **Images** (once per Legion release): the `js` and `python` images ship
   pre-initialized with the engine booted and the standard library loaded.
2. **Entries** (once per job config per node, at stage time): when a task
   names `entry`, the stager runs the image with the author's script under
   Wizer and caches the result as the task's own artifact. Initialization
   runs with no I/O imports linked, so a script that fetches at top level
   fails at stage with a named error rather than baking a response into the
   snapshot. Every instance then reseeds randomness and re-reads the clock
   at its first turn, the isolate version of a VM fork's identity refresh.

**Where compilation runs.** wasmtime splits cleanly: a runtime-only build
that loads precompiled `.cwasm` artifacts adds 1.2 to 2.1 MB; Cranelift is
the rest of the default 19 MB
([minimal builds](https://docs.wasmtime.dev/examples-minimal.html)).
Loading an artifact is `unsafe` by design: a tampered `.cwasm` is
arbitrary code execution, so the artifact is a trust boundary.

| Option | For | Against | Verdict |
| --- | --- | --- | --- |
| **A short-lived compiler process on the node** (`legion-isolate compile`), artifacts kept node-local in the content store | The artifact never crosses a trust boundary; the host process never compiles; a compile that blows up dies alone with its own seccomp and memory limit; every component is validated by the latest `wasmparser` first, which alone would have stopped CVE-2026-115384 | Every node compiles each module once | **Take** |
| Compile on the manager (or a `legion-system` app), signed artifacts distributed through the content store | One compile per cluster per module; workers could be runtime-only | Signature verification becomes the only thing between a bucket writer and code execution on every node; a dynamic module pays a network round trip | Defer, as an optimization for large fleets with few modules |
| Compile inside the host process | No IPC | A compiler in the multi-tenant process, and a compile on a shard thread stalls its isolates | Reject |
| Accept user-supplied `.cwasm` | Zero compile | Arbitrary code execution by design | Reject outright |

**Which compiler.** Cranelift only, on x86-64 first.

| Tier | Compile | Code | Notes | Verdict |
| --- | --- | --- | --- | --- |
| **Cranelift** | baseline | 1.5 to 2.5× native on crypto-heavy code | Most fuzzed, formally verified lowering rules in progress (Crocus, Arrival) | **Take** |
| Winch (baseline) | ~7 to 20× faster | ~1.1 to 1.9× slower than Cranelift | **Incompatible with epoch interruption** (needs fuel), and a 2026 sandbox escape ([CVE-2026-34987](https://github.com/bytecodealliance/wasmtime/security/advisories/GHSA-xx5w-cvp6-jv83)) | Reject for untrusted code until both change |
| Pulley (interpreter) | about as slow as Cranelift (Cranelift emits its bytecode) | ~10× slower | Portable; no fast-load benefit | Reject |
| wasmi beside wasmtime for first runs | none | ~10× slower | Two engines' bugs in one trust boundary for a rare path (dynamic *Wasm*; dynamic code is mostly JS and Python source, which needs no compile) | Reject |

The two critical aarch64 escapes of April 2026 and the maintainers' note
that OSS-Fuzz has no aarch64 runners make x86-64 the first supported host
architecture; aarch64 follows when continuous fuzzing covers it (§6.10).

### 6.6 Waiting without holding anything

This is the property the user asked for in so many words: isolates cede
resources during async calls. What an awaiting isolate can hold is a
thread, a stack, and memory. §6.3 already takes the thread: a shard runs
another isolate while one waits. This section is about the stack; §6.7 is
about memory.

| Option | What a waiting isolate holds | Guests it works for | Maturity | Verdict |
| --- | --- | --- | --- | --- |
| **Stackless: the Component Model async callback ABI** (WASI 0.3) | Nothing but its linear memory and a task record: the guest returns to the host's event loop between turns, clearing its native stack | Guests built for it: Rust (wit-bindgen async), JS (jco), and any guest whose language already has an event loop | WASI 0.3.0 shipped 2026-06-11; `component-model-async` Tier 1 and on by default since Wasmtime 46; a critical guest-controlled write to the host's native stack in this path was fixed 2026-10-02 ([CVE-2026-115384](https://github.com/bytecodealliance/wasmtime/security/advisories/GHSA-32h6-97mm-8q3c)) | **Take as the target** |
| **Fibers: `call_async` on a stack per in-flight call** | A fiber stack (default 2 MiB of address space, tunable to 128 KiB; resident only as touched) | Every guest, including WASI 0.2 guests that block in a sync import | Mature; how wasmtime has done async since 2021 | **Take as the fallback** for 0.2 guests |
| Wasm stack switching | A continuation | Future toolchains | Phase 3 proposal; not tiered in wasmtime | Wait |
| Asyncify-style transforms | Nothing native, but code size and speed costs | Any | Mature, slow | Reject |

**The host drives each guest's event loop.** For a stackless guest, every
event (the request arrived, a fetch completed, a timer fired) is a turn: the
host calls the guest's callback, the guest runs until it awaits, returns
`wait` or `yield` with what it waits on, and the host parks it. The
guest's own continuation state is in its linear memory, where a JS engine
or a Rust future keeps it anyway. Between turns the isolate is data, not
an execution, which is what makes hibernation (§6.7) possible at any
await, not only when idle.

**Turns are short by construction, and preempted when not.** Each turn
runs under an epoch deadline. A ticker thread advances the engine epoch
every millisecond; a turn that exceeds its slice (default 5 ms) yields to
the shard (`epoch_deadline_async_yield_and_update`) rather than trapping,
so a CPU-bound guest is time-sliced fairly, and the invocation's CPU
budget is checked at each yield. Epochs cost about 10% on compute-heavy
code and are 2 to 3× cheaper than fuel; an MMU-based variant under review
cuts SpiderMonkey's overhead from 14.4% to 2.8%. Fuel is used only where
an operator asks for exact instruction counts. Bulk operations
(`memory.copy`) check once, so the memory cap also bounds the longest
uninterruptible stretch.

**Fairness across tenants** is deficit round robin per shard, keyed by
(job, tenant key), weighted by the job's envelope CPU, so one tenant with
10,000 runnable isolates gets the same share as one with ten.

### 6.7 Ceding memory: four resident tiers

A waiting isolate holds no thread and (stackless) no stack, but it still
holds its private pages. At 10^5 isolates of 1 MiB that is 100 GiB.
Density at rest therefore needs memory to leave, too, in tiers keyed off
how long the isolate has been idle (or waiting):

| Tier | What it holds | Wake cost | Entered |
| --- | --- | --- | --- |
| **Hot** | A slot, its private pages resident | none | running, or idle < `pageOutAfter` (default 2 s) |
| **Paged out** | A slot; its private pages pushed to the kernel's compressed swap (zswap/zram) or swap with `MADV_PAGEOUT` | Page faults: ~µs per page, typically ≤ 200 µs for a small isolate | idle ≥ `pageOutAfter`, or node memory pressure |
| **Hibernated** | No slot. Its private pages (diffed against the artifact's image), globals, table state, and the host-side resource table, compressed in a file under the node's state dir | A slot, the CoW image, the diff read back: ≤ 2 ms | idle ≥ `suspend.idle.after`, slot pressure, or memory pressure; only between turns |
| **Evicted / published** | Nothing on the node (`local`), or a manifest and parts in the content store (`store`) | As for a session resume | `suspend.keepFor` passes; `durability = "store"` publishes at hibernation |

- **Hibernation needs a turn boundary.** A stackless isolate is at one
  whenever it waits. A fiber-based isolate is at one only when it has no
  call in flight. That is the practical reason stackless is the target:
  with fibers, "waiting on a 20 s LLM call" pins a slot and a stack; with
  callbacks it can hibernate while it waits.
- **Host-held resources outlive the guest.** Open sockets, WebSockets, and
  timers belong to the host (the guest holds handles). A hibernated session
  isolate's WebSocket stays open, and a message wakes it, which is
  Cloudflare's WebSocket hibernation for Durable Objects.
- **Memory pressure learns one step.** The worker's named pressure steps
  (`src/worker/pressure.rs`) gain "hibernate isolates", ordered before
  stopping anything: the cordon pages out, then hibernates, its least
  recently used isolates.
- **Paging needs the node to have somewhere to page.** Without zswap,
  zram, or swap, `MADV_PAGEOUT` does nothing for anonymous pages and the
  paged-out tier collapses into hot. The public doc says so and the worker
  reports it.

### 6.8 Instance lifetime

| Lifetime | What a call gets | State between calls | Isolation between calls | Verdict |
| --- | --- | --- | --- | --- |
| **`request`** (Fastly's model) | A fresh instance, destroyed after | None | Total: nothing survives a request | **Default** |
| **`tenant`** (Workers' model) | A resident instance per (job, tenant key), reused while warm | Globals persist, best effort | Within one tenant only | Opt-in, for images whose per-call setup is costly (Python) |
| **`session`** (Durable Objects' model) | An addressable instance with a session id | Durable across hibernation; `store` survives the node | One session | Opt-in, for agents and actors |

`request` is the default because it is also a security property: an
instance that lives for one request cannot accumulate a side channel's
bits across requests, and a bug that corrupts it dies with it. Fastly
argues exactly this, and the 2026 Spectre result on Workers needed a
connection held open for hours to work.

### 6.9 The host API

The world in §7.4. What is in it, and what is deliberately not:

| Capability | Provided as | Not provided |
| --- | --- | --- |
| Inbound HTTP | `wasi:http/incoming-handler`, dispatched from node ingress with no TCP hop | Raw listeners |
| Outbound HTTP | `wasi:http/outgoing-handler`, carried to the worker's egress datapath | Raw sockets, DNS |
| Files | `wasi:filesystem` over an in-memory scratch tree, counted in the memory cap, gone at the end of the instance | A persistent or host filesystem |
| State | `legion:isolate/state`: bytes scoped to the tenant or session, kept in the hibernation file and the session manifest | Cluster KV writes (KV is raft-replicated; the hot path must not touch it) |
| Config | Env from the jobspec; KV-rendered env at stage, as for other runtimes | Secrets: credentials are injected on egress, never handed in |
| Clocks | Wall clock at 1 ms; both clocks frozen within a turn | High-resolution timers, `SharedArrayBuffer`, threads |
| Logs | stdout/stderr to the task's log stream with an `isolate` field, and into `RunIsolate`'s reply | A log file per isolate |
| A VM | `legion:isolate/vm` (§6.12) | Processes in the isolate |

**I/O goes through the worker, attributed per isolate.** The host process
has no network interface. Every outbound request goes over the host's
socketpair to the worker as HTTP/2 with a PROXY v2 header naming the job,
allocation (the envelope), and isolate, the same TLVs the datapath writes
today (`src/worker/netd/proxyproto.rs`). The worker applies the job's
`connect` block, egress policy, handlers, and credential injection as it
does for a gated allocation. The isolate never sees a credential and
cannot reach a destination its job's policy does not allow.

| Option | For | Against | Verdict |
| --- | --- | --- | --- |
| **The worker does egress** | The host holds no secrets and no network; one policy engine | One extra local hop (~10 to 30 µs) and worker CPU per request | **Take** |
| The host does egress under pushed policy | One hop fewer | The multi-tenant process holds credentials and network access, the two things Google says sensitive data must be kept away from | Reject |
| A network namespace per isolate | Kernel-enforced | Impossible at 10^5 per node | Reject |

**Rejected: POSIX emulation (WASIX).** It would make more code run in
isolates, and every syscall it emulates is host attack surface and a
reason an isolate never bursts when it should. Code that needs POSIX gets a
VM, which already is POSIX.

### 6.10 Security model and defense in depth

The 2026 evidence sets the bar. Researchers leaked a JWT across tenants
from Cloudflare's production Workers at up to 12 bits a second, using a
remote timer over a WebSocket, which defeats frozen local clocks
([Cloudflare, 2026-08-19](https://blog.cloudflare.com/revisiting-spectre-attacks-on-workers),
[paper](https://arxiv.org/abs/2608.17043)). Wasmtime published five
critical sandbox escapes in 2026, most found with LLM tooling, after none
in 2024 and 2025 ([BA, 2026-04-09](https://bytecodealliance.org/articles/wasmtime-security-advisories)).
Google's position is that Spectre cannot be mitigated in software inside
one process: "the only effective mitigation is to move sensitive data out
of the process's address space" ([v8.dev](https://v8.dev/blog/spectre)).
So the engine is one wall among several, and the design keeps secrets out
of the shared process altogether.

**Layers, inside out:**

1. **Wasm software fault isolation**, Cranelift on x86-64, guard-page
   layout, Spectre mitigations on (index masking for tables and
   `br_table`, explicit-check `cmov` where checks are explicit).
2. **Language engines inside the sandbox.** JS and Python interpreters run
   in Wasm, so an engine bug (the class behind most V8 exploits) corrupts
   only that isolate's linear memory. JS also runs without a JIT, which is
   what Google advises high-risk users do with V8.
3. **Instance lifetime.** `request` by default (§6.8).
4. **Clocks and concurrency.** Coarse wall clock, both clocks frozen in a
   turn, no wasm threads or shared memory. Known to be insufficient alone
   (the remote-timer result), kept because it raises the cost.
5. **No secrets in the process.** Credentials are injected by the worker
   on egress. The host has no TLS keys, no tokens, no KV access. What a
   cordon holds is its tenants' data, which is why the cordon is the trust
   unit.
6. **Cordons.** Tenants of different trust never share a process: by
   default one per namespace; `job` for sensitive work; `shared` only
   where an operator opts in.
7. **The host process is confined.** Its own user namespace and
   unprivileged uid; an empty network namespace (no interface, not even
   loopback); a seccomp **allowlist** (unlike the containers' denylist) of
   roughly what a reactor needs: `futex`, `mmap`/`munmap`/`madvise`/`mprotect`,
   `read`/`write`/`recvmsg`/`sendmsg` on inherited descriptors,
   `epoll`/`io_uring`, `clock_gettime`, thread `clone` only, `exit`;
   Landlock read-only on its artifact directory and nothing else; no
   `execve`; `no_new_privs`; and a cgroup with the cordon's `memory.max`,
   `cpu.weight`, and `pids.max`. Cloudflare's layer-2 sandbox is the same
   shape: namespaces and seccomp, no filesystem, Unix sockets only, egress
   through a proxy, code fetched by a supervisor.
8. **No compiler in the host, and validation before compile.** Artifacts
   come from the node-local compiler process (§6.5), which validates each
   component with the newest `wasmparser` before compiling: the published
   mitigation for 2026's async-callback stack overflow, applied to every
   module. The host role never calls Cranelift. Denying
   anonymous executable memory outright needs an LSM (SELinux `execmem`);
   seccomp cannot tell a file mapping from an anonymous one in `mprotect`,
   so this is a goal under seccomp alone and a guarantee only with the LSM
   (Phase 6).
9. **Optional VM boundary per cordon** (`cordonBoundary = "vm"`): the
   cordon's host runs inside a microVM from the sandbox runtime, so an
   escape plus a kernel exploit still lands in a guest. workerd's README
   says to run it inside a VM, and Deno runs its isolate pools in VMs.
   Costs a guest kernel per cordon and a vsock hop for I/O.
10. **Detection and quarantine** (Phase 6). Tenants that trap repeatedly,
    hit CPU limits, or show anomalous branch-miss ratios on performance
    counters are moved to a quarantine cordon. Cloudflare's dynamic process
    isolation missed the 2026 attack because it judged only finished
    invocations; Legion's judges running ones too, and treats long-lived
    connections as a co-location risk.
11. **Patch speed.** `legion-isolate` is its own binary, upgradeable
    without the worker, and `legion operator isolate disable` (§13) turns
    the tier off cluster-wide while an advisory is open. Cloudflare's V8
    patch gap is under 24 hours; that is the target.

**What this design does not claim:** that tenants in one cordon are safe
from a determined Spectre attacker. It claims that such an attacker
reaches only data in that cordon, never a credential, and that an
operator decides who shares a cordon. The public doc must say this in
those words.

### 6.11 Limits and accounting

- **Per invocation:** `limits.cpu` (turn CPU summed, checked at every
  yield), `limits.wall`, `limits.output`. Each refusal is a typed error
  (`isolate_limit` with which limit), and each is a `burst.on = "limits"`
  trigger.
- **Per isolate:** `limits.memory`, the hard cap on linear memory, enforced
  by the pooling allocator's limiter. A guest that asks for more gets a
  failed `memory.grow`, then usually a trap.
- **Per job on a node:** the envelope. The host admits an isolate when the
  envelope's room covers its *charge* (`resources.memory`, the expected
  private memory), not its cap, and keeps a measured per-image estimate of
  private pages so an operator can see when the charge is wrong. Two
  numbers on purpose: a tenant gets a predictable cap, and the node packs
  by what isolates actually use.
- **Per cordon:** the kernel. The cgroup's `memory.max` is the sum of the
  envelopes it serves; per-job memory inside a shared host is the host's
  accounting, not the kernel's (§14 Q7).
- **Concurrency:** active invocations per job per node are bounded by the
  envelope CPU and a queue (`RESOURCE_EXHAUSTED`, retryable, after
  `edgeBoot.queueTimeout`), so overload sheds before it queues forever.

### 6.12 Burst to a VM

The VM tier exists and is not redesigned here. This section decides how
code gets from an isolate to a VM, when, and with what.

**Models.**

| Model | What happens | For | Against | Verdict |
| --- | --- | --- | --- | --- |
| **A. Attached VM** | The isolate calls `vm.acquire()`, gets a handle to a microVM of the burst image on the same node, and drives it: `exec`, `put`/`get` files, `fetch` its ports. The isolate stays in charge and gets results back | Explicit, composable, what Project Think, Cloudflare Containers, and Vercel all do; reuses exec and gated networking unchanged | The author writes the escalation | **Take** (the primitive) |
| **B. Replay** | The invocation fails in the isolate (`unsupported` or `limits`), and Legion runs the same source and input in a VM, answering the original call | Zero code changes; the caller sees one call that worked | Running code twice is wrong if the first run had side effects | **Take, narrowly**: only for language images and `wasi:cli/run` components, and by default only if the isolate made no egress (`replay = "before-egress"`) |
| **C. Continuation migration** | Hibernate the isolate at a turn boundary and resume the *same* instance inside a VM-hosted `legion-isolate` with real files, sockets, and processes | The most magical: state and progress move | Nobody ships it; host-held resources must be re-bound across a machine boundary; engine versions must match exactly | **Defer** to Phase 8, research |
| **D. Route first** | Choose the tier before running: a pre-flight scan of the source's imports against the image, plus a per-node memo of code hashes that burst before | Avoids a wasted isolate run; cheap | Heuristic | **Take**, as the `tier = "auto"` default |

**Triggers** (`burst.on`):

| Trigger | Raised by | Replayable |
| --- | --- | --- |
| `requested` | The guest calls `vm.acquire()` | n/a (model A) |
| `unsupported` | An image stub reports `IsolateUnsupported` (§6.2), or the pre-flight check finds an import the image lacks | Yes, before egress |
| `limits` | CPU, wall, or memory limit | Yes, before egress |
| (always) | The operator's kill switch is on and `requested` is allowed | Yes |

**What the VM is.** A session of a generated task, `<task>-burst`, on the
`sandbox` runtime, built from `burst.sandbox` and `burst.resources`, with
`burst.envelope` as its envelope and the isolate task's `connect` and
`network` blocks. So a burst VM reaches the same upstreams with the same
injected credentials, under the same gated policy, and its files and exec
are the sandbox runtime's, unchanged. It runs on the isolate's node when
the burst envelope there has room (files then move over the node, not the
network), and anywhere the router finds room otherwise.

**How fast it gets there**, in three steps that each ship alone:

1. **Cold boot** (works today): the sandbox's 0.18 to 0.23 s from create
   to a guest taking exec, from the cached root disk.
2. **Restore from a warm base snapshot** (needs
   [scaling plan Phase 8](sandbox-scaling-plan.md): sessions started from
   a snapshot): each node keeps a memory snapshot of the booted burst
   image; a burst restores it, Firecracker sharing clean pages
   `MAP_PRIVATE`. Restores in the 4 to 10 ms class are published for
   Firecracker and Unikraft.
3. **A pool of prepared, unassigned VMs** (Cloudflare's 2026 design): the
   node keeps a few paused, restored VMs per burst image and assigns one on
   burst. Assignment runs the guest agent's existing resume handler
   (identity, RNG reseed, clock, `onResume`), as a fork does today. Target
   ≤ 10 ms p50 on metal. Sized by the burst envelope's floor; refilled off
   the critical path.

**What crosses.** Files the isolate wrote to its scratch tree go over with
`vm.put` (model A) or are copied in before replay (model B), along with
the source and the input. Environment and network rules are the task's.
Memory never crosses (that is model C). Output comes back as the
`RunIsolate` reply (B) or as `exec` results (A).

**Lifetime and cost.** An acquired VM lives until the isolate releases it,
its invocation ends (`request`), or its session ends; the burst task's
`suspend.idle` applies in between. A replayed run's VM is released when it
answers. Bursts draw from the burst envelope, never from the isolate
envelope, so a storm of bursts cannot starve isolates, and a full burst
envelope fails the burst with `RESOURCE_EXHAUSTED` (retryable) rather than
queueing behind VM boots.

**The memo.** A node remembers, per (job, code hash), that a run burst and
why, for `burst.memoFor` (default 1 h), and routes the next run of the same
code straight to a VM. It is node-local and advisory: another node
relearns with one wasted isolate run.

### 6.13 Observability

Per task, never per isolate, on the existing metric families' pattern:
`legion_isolate_instantiate_seconds{image}`, `legion_isolate_turns_total`,
`legion_isolate_cpu_seconds_total`, `legion_isolate_resident{tier}`,
`legion_isolate_wakes_total{from}`, `legion_isolate_limit_total{limit}`,
`legion_isolate_bursts_total{reason}`, `legion_isolate_compile_seconds`,
and per cordon `legion_isolate_host_rss_bytes` and slot occupancy. One
log line per burst and per limit refusal; none per invocation by
default.

## 7. The surface

### 7.1 The jobspec

An isolate task is a session task on the `isolate` runtime. Everything
under `config` is new; `stateful`, `resources`, `connect`, `network`, and
`suspend` mean what they mean today.

```nickel
{
  name = "code-runner",
  tasks = [{
    name = "run",
    runtime = "isolate",
    config = {
      # What runs: a built-in image ("js", "python") or a component.
      image = "python",
      # entry = "main.py",           # pre-initialized at stage time; omit
      #                               # for dynamic code (RunIsolate)
      # module = { source = "oci://ghcr.io/acme/handler:1.4", sha256 = "..." },
      lifetime = "request",           # "request" (default) | "tenant" | "session"
      limits = {
        cpu = "100ms",                # CPU per invocation (turns summed)
        wall = "30s",                 # wall clock per invocation
        memory = "128MiB",            # linear memory cap per isolate
        output = "1MiB",              # stdout + stderr returned per run
      },
      cordon = "namespace",           # "namespace" (default) | "job" | "shared"
      burst = {
        on = ["requested", "unsupported", "limits"],
        replay = "before-egress",     # "never" | "before-egress" | "always"
        sandbox = {                   # a `sandbox` runtime config, unchanged
          image = "python:3.13-slim",
          vcpus = 1,
        },
        resources = { cpu = 1000, memory = 1024 },          # per VM
        envelope = { resources = { cpu = 8000, memory = 16384 },
                     max = { cpu = 32000, memory = 65536 } }, # per node
        memoFor = "1h",               # route a code hash that burst straight to a VM
      },
    },
    # The admission charge per isolate: what the node assumes one costs.
    # The hard cap is config.limits.memory. See §6.11.
    resources = { cpu = 5, memory = 2 },
    stateful = {
      mode = "session",
      envelope = { resources = { cpu = 16000, memory = 32768 } },
    },
    connect = {
      upstreams = [{
        name = "openai",
        to = "https://api.openai.com",
        credentials = [{ header = "Authorization", prefix = "Bearer ",
                         from = { kv = "secrets/openai" } }],
      }],
    },
  }],
}
```

What submit checks, with the errors it gives:

```text
[isolate] code-runner/run: runtime "isolate" needs stateful.mode = "session" (isolates are node-owned and never written to the store; see docs/task-runtime-isolate.md)
[isolate] code-runner/run: config.limits.cpu and config.limits.wall are required (budgets differ by orders of magnitude between a webhook and an agent loop; there is no default)
[isolate] code-runner/run: config.limits.memory 8GiB is above this cluster's isolate maximum 4GiB (worker isolate.maxMemory); burst to a VM for more
[isolate] code-runner/run: config.burst.on includes "limits" but config.burst.sandbox is not set
[isolate] code-runner/run: config.cordon = "shared" is refused by namespace policy 'tenants' (isolateCordons = ["namespace", "job"])
```

An HTTP handler declares a port, and ingress dispatches into the host
without a TCP hop:

```nickel
network = { ports = [{ name = "http" }] },
```

### 7.2 The CLI

```console
$ legion isolate run code-runner --file snippet.py -j
{"ok":true,"data":{"tier":"isolate","node":"w-17","exit":0,
 "stdout":"42\n","stderr":"","cpuMs":3.1,"wallMs":4.0,"burst":null,
 "deduplicated":false}}

$ legion isolate run code-runner --file transcode.py --idempotency-key t-81 -j
{"ok":true,"data":{"tier":"vm","node":"w-17","exit":0,"stdout":"done\n",
 "burst":{"reason":"unsupported","detail":"subprocess.run: processes are not available in an isolate",
          "replayed":true,"vmMs":231},"cpuMs":1.2,"wallMs":1890,"deduplicated":false}}

$ legion isolate run code-runner --file loop.py -j
{"ok":false,"error":{"code":"isolate_limit",
 "message":"isolate exceeded its 100ms CPU limit (100.4ms) and burst.on does not include \"limits\"",
 "hint":"raise config.limits.cpu, or add \"limits\" to config.burst.on to rerun it in a VM",
 "retryable":false}}

$ legion isolate stats code-runner -j
{"ok":true,"data":{"task":"run","nodes":[
  {"node":"w-17","resident":48213,"active":312,"pagedOut":20114,"hibernated":391022,
   "invocationsPerSec":8410,"cpuSecondsPerSec":11.2,"burstsPerMin":14,
   "cordon":"default","hostRssBytes":21474836480}]}}

$ legion isolate validate code-runner --file snippet.py -j   # pre-flight only
{"ok":true,"data":{"tier":"isolate","imports":["json","csv"],"reason":null}}
```

`run` also takes `--source -` (stdin), `--input` (JSON handed to the
entry point), `--tier auto|isolate|vm` (default `auto`), `--node` with
`--ticket` for an edge call, and `--session <id>` to run in an existing
session isolate. `validate` runs only the pre-flight check (§6.12) and
never executes code.

### 7.3 The RPCs

- **`Manager.RunIsolate`** (`job:write` on the job). The manager's session
  router picks a node with envelope room, exactly as for an unnamed
  session create, and forwards. Nothing is written.
- **`Worker.RunIsolate`** on the worker-plane port with a session ticket,
  for edge callers with no manager on the path (the edge start's shape).
- **`Worker.GetIsolateStats`** and the heartbeat's per-task aggregates
  behind `legion isolate stats`.

`RunIsolateRequest`: `job_id`, `task_name`, `source` or `input`,
`tier`, `session_id`, `idempotency_key`, `wait_timeout`. `RunIsolateResponse`:
`tier`, `node_id`, `exit`, `stdout`, `stderr` (bounded by
`limits.output`, with `truncated`), `result` (JSON the entry returned),
`cpu_ms`, `wall_ms`, `burst { reason, detail, replayed, vm_ms }`,
`deduplicated`. Idempotency is per node, as for edge session starts: the
key derives an invocation id, and the node keeps the reply for 10 minutes.

### 7.4 The guest's world

The interface a component is written against, in WIT. Imports a world
does not use are not linked, so a component that imports something the
host does not provide fails at deploy, not at the call.

```wit
package legion:isolate@0.1.0;

interface vm {
  resource machine {
    exec: func(argv: list<string>, stdin: list<u8>, timeout-ms: u32)
          -> result<exec-result, error>;
    put: func(path: string, bytes: list<u8>) -> result<_, error>;
    get: func(path: string) -> result<list<u8>, error>;
    fetch: func(port: u16, request: outgoing-request) -> result<incoming-response, error>;
  }
  acquire: func(profile: option<string>) -> result<machine, error>;
}

world handler {
  import wasi:clocks/monotonic-clock@0.2.0;   // frozen within a turn
  import wasi:clocks/wall-clock@0.2.0;        // coarsened to 1 ms
  import wasi:random/random@0.2.0;
  import wasi:cli/environment@0.2.0;          // the task's env
  import wasi:cli/stdout@0.2.0;               // to the task's log, and the run's reply
  import wasi:cli/stderr@0.2.0;
  import wasi:filesystem/types@0.2.0;         // in-memory scratch, counted in the memory cap
  import wasi:http/outgoing-handler@0.2.0;    // through the node egress datapath
  import legion:isolate/state;                // tenant- or session-scoped bytes
  import legion:isolate/vm;                   // burst
  export wasi:http/incoming-handler@0.2.0;    // or wasi:cli/run for commands
}
```

The same world exists in its WASI 0.3 form (async functions on the
stackless ABI) once the toolchains it needs support it (§6.6).

### 7.5 Agents

- An MCP tool `isolate_run` (`mutates: true`, so behind `--allow-write`)
  and `isolate_validate` (read-only), wrapping the RPCs.
- `catalog()` entries in `src/cmd/capabilities.rs` for `isolate run`
  (`mutates: true`, `read_only_safe: false`), `isolate validate`,
  `isolate stats`, and `operator isolate disable|enable` (`mutates: true`).
  The kill switch is deliberately not an MCP tool.
- `docs/agent-readiness.md` gains the isolate surface, including the
  per-node idempotency rule.

## 8. The control plane (kept small on purpose)

Isolates add no placement model. An `isolate` task is a session task:

1. **Envelopes reserve the room.** Submit generates `<task>-envelope` as
   for any session task. The envelope stays a reservation and runs
   nothing; the cordon host is node infrastructure, started by the worker
   when the first envelope that needs it lands, and its cgroup's
   `memory.max` and `cpu.weight` are the sum of the envelopes it serves.
   Burst generates a second session task, `<task>-burst`, on the `sandbox`
   runtime, with `burst.envelope` as its (elastic) envelope.
2. **Routing is the session router's.** `RunIsolate` and HTTP ingress go
   to any node with envelope room, picked by `stateful.placement`. A
   session isolate's id carries its node tag, so a later call goes
   straight to its node, through the router, the gateway, or an edge
   caller.
3. **Starting an isolate takes no launch slot.** The launch pool bounds VM
   boots. An isolate start is admitted against the envelope by the host
   itself, and the host's own concurrency limits apply (§6.11). A burst VM
   takes a launch slot like any sandbox session.
4. **Nothing per isolate leaves the node.** The heartbeat carries one
   aggregate per isolate task (`resident`, `active`, `paged_out`,
   `hibernated`, invocation and CPU counters, bursts, refusals), folded by
   the manager like usage. A session isolate is not in the managers'
   session index; `legion isolate get <id>` asks the node its tag names.
5. **Hibernated session isolates follow session sleep rules.**
   `suspend.durability = "local"` keeps the hibernation file on the node;
   `"store"` publishes it and writes a session manifest, so the session can
   wake on another node.

What the managers never see: an isolate's start, end, invocation,
page-out, or hibernation.

## 9. Proto, struct, config, and persistence changes

- **Proto.** `Manager.RunIsolate`, `Worker.RunIsolate`,
  `Worker.GetIsolateStats`, `Manager.SetIsolateTier` (the kill switch,
  `operator:write`), `IsolateTaskStats` on the heartbeat's usage report, and the request/response messages in §7.3, each with full public
  proto comments.
- **Job config.** `config` for the `isolate` runtime (§7.1), validated by
  the runtime's schema. New fields go into `configs/jobs/maximal.ncl` with
  the runtime.
- **Namespace policy.** `isolateCordons` (allowed cordon kinds) next to
  `runtimes`.
- **Worker config.** An `isolate` block: `binary` (default: beside the
  worker, then `LEGION_ISOLATE_BINARY`), `maxMemory` (per isolate, default
  4 GiB), `slotsPerHost`, `shards` (default: cores in the host's cpuset),
  `epoch` (default `1ms`), `slice` (default `5ms`), `pageOutAfter`
  (default `2s`),
  `hibernation = { dir, budget }`, `cordonBoundary = "process" | "vm"`.
- **Persistence.** None in the store. On the node: the compiled-module
  cache under the content store (keyed by module digest, engine version,
  and CPU feature set), and hibernation files under
  `<worker state dir>/isolates/`. Session isolates in `store` durability
  reuse session manifests.
- **Struct.** `IsolateTaskConfig`, `IsolateLimits`, `IsolateBurstConfig`,
  `IsolateStats` in `src/structs/`.

## 10. Target numbers per phase

Targets assume a 16-vCPU, 64 GiB x86-64 node with 4-level paging and
MPK, and are replaced by Phase 0's measurements.

| Phase | Start p50 | Resident per node | Waiting cost | Burst |
| --- | --- | --- | --- | --- |
| 0: measure | measured per image | measured bytes per idle isolate per image | measured fiber vs stackless | measured cold boot |
| 1: isolate runtime, one job per host | ≤ 20 µs (component) | ≥ 1.6×10^4 per host process (guard-page slots) | one fiber per in-flight call | none |
| 2: shared hosts, cordons | unchanged | unchanged per host, any mix of jobs | unchanged | none |
| 3: images and dynamic code | ≤ 300 µs JS pre-init, ≤ 1 ms JS dynamic, ≤ 5 ms Python | ≥ 500 JS per GiB | unchanged | none |
| 4: sessions and memory tiers | wake from page-out ≤ 200 µs, from hibernation ≤ 2 ms | ≥ 10^5 resident, ≥ 10^6 hibernated | no stack for stackless guests | none |
| 5: burst | unchanged | unchanged | unchanged | ≤ 250 ms cold, ≤ 50 ms warm (needs scaling Phase 8) |
| 6: density and hardening | unchanged | ≥ 2×10^5 resident (MPK striping) | unchanged | unchanged |

## 11. Implementation phases

Each phase ships on its own and is useful on its own.

- **Phase 0: measure before building.** A spike binary, not merged into
  the worker, on the AWS rig's instance types (nested and metal): wasmtime
  pooling instantiation p50/p99 for a Rust component, the StarlingMonkey
  and QuickJS images, and CPython; private memory per idle instance per
  image; slots per process with guard pages, with MPK striping, and with
  small reservations; epoch-check overhead on a CPU benchmark; fiber
  versus stackless suspend and resume; V8 isolate creation and floor
  through the `v8` crate for the same JS, so the Phase 7 call rests on our
  own numbers; `legion-isolate` binary size with and without Cranelift;
  whether an idle `Store` keeps its worker fiber (and so a stack slot) on
  the callback ABI; whether the pool can place slots above 47 bits on a
  5-level-paging kernel; and Python's start with the standard library
  pre-initialized. Output:
  a findings doc and §4/§10 rewritten to measured numbers.
- **Phase 1: the `isolate` runtime, one job per host.** `legion-isolate`
  (host and `compile` subcommand), one host per (job, node) envelope, the
  shard model, pooling allocator with copy-on-write images, epoch
  preemption, limits, `wasi:cli/run` commands and `wasi:http` handlers
  from precompiled components, request lifetime, logs, egress through the
  worker datapath with PROXY v2 attribution, ingress dispatch, `isolate
  run`/`validate`/`stats`, the RPCs, catalog and MCP entries, and the
  public page `docs/task-runtime-isolate.md`. Closes the TODO "WASM
  runtime" item.
- **Phase 2: shared hosts and cordons.** Many jobs per host by cordon
  policy, per-job admission and per-tenant fair scheduling inside a host,
  the host's seccomp allowlist, Landlock, empty network namespace, and
  user namespace, namespace policy `isolateCordons`, and the kill switch
  (§13).
- **Phase 3: images and dynamic code.** The `js` and `python` images,
  stage-time pre-initialization of `entry`, dynamic snippets through
  `RunIsolate`, the tiered compile pipeline for dynamic Wasm, and the
  pre-flight import check.
- **Phase 4: session isolates and memory tiers.** `lifetime = "tenant" |
  "session"`, the stackless async world, page-out and hibernation,
  host-held connections, gateway wake, `durability = "store"` through
  session manifests, and the memory-pressure step.
- **Phase 5: burst.** `legion:isolate/vm`, the generated `<task>-burst`
  task, replay before egress, the per-code-hash burst memo, and warm
  restores once scaling Phase 8 lands.
- **Phase 6: density and hardening.** MPK striping, small-reservation
  slots, 5-level paging where the kernel offers it, `cordonBoundary =
  "vm"`, SELinux `execmem` denial for hosts, perf-counter quarantine, and
  the Spectre-mode knobs.
- **Phase 7 (conditional): a V8 engine** for JS tenants, built only when
  Phase 3's telemetry shows the trigger in calls entry 1.
- **Phase 8 (research): continuation migration**, resuming a hibernated
  isolate inside a VM-hosted `legion-isolate` with the full filesystem and
  process capabilities (§6.12, model C).

## 12. Testing plan

- **Unit (Phase 1+).** Limits arithmetic and trap classification
  (`describe_failure`'s successor for wasmtime traps, epoch deadlines,
  memory refusals); admission against an envelope with the cap and the
  charge distinct; the shard scheduler's fairness (deficit round robin
  across tenants under a synthetic load, asserting CPU shares within 5%);
  idempotent invocation ids; the egress attribution header round trip.
- **Integration (`tests/integration/isolate.rs`, one module of the shared
  binary).** A component answers HTTP through ingress; a component's
  outbound request reaches an upstream with an injected credential the
  guest never sees; CPU, wall, memory, and output limits each produce
  their error envelope; a host process killed mid-invocation fails its
  in-flight runs with a retryable error and the worker restarts it; a job
  in one cordon cannot be scheduled into another's host; `RunIsolate`
  through the manager and at the edge with a ticket; the ACL table entry
  for every new Manager RPC in `tests/integration/acl_endpoints.rs`.
- **Confinement (Phase 2).** A test build of the host with a hook that
  attempts each forbidden syscall (`socket`, `openat` outside the cache,
  `execve`, `ptrace`, anonymous `PROT_EXEC`) and asserts the kill; the
  host's network namespace has no interface.
- **Guest-escape regression corpus.** Known-bad modules (out-of-bounds
  loads at every width, table index masking cases, the CVE-2023-26489
  reproduction) run against every engine upgrade in CI.
- **Fuzzing.** `cargo fuzz` targets for each host function's argument
  decoding and for the WIT boundary, run nightly.
- **Benchmarks.** `benchmarks/micro/isolate_bench.rs`: instantiate per
  image, turn switch, page-out and wake, hibernate and wake, egress round
  trip through the worker. Scheduler-style before/after numbers in every
  PR that touches the host's hot path.
- **Density campaign (Phase 4, 6).** An AWS run that fills a node with
  session isolates to its memory budget and reports resident, paged, and
  hibernated counts, wake latencies, and host RSS per isolate.
- **Burst (Phase 5).** Ignored-by-default KVM tests: `vm.acquire` and exec,
  replay before egress, refusal of replay after egress, burst memo.

## 13. Rollout and backward compatibility

- **New surface only.** No existing job, runtime, or default changes.
  Network plugins keep wasmi.
- **Default on where it can run.** The runtime loads on a worker that finds
  `legion-isolate` beside it (or through `LEGION_ISOLATE_BINARY`), as the
  sandbox runtime loads where it finds `legion-guest`. A worker without the
  binary does not advertise the `isolates` capability, so envelopes are
  not placed there. A namespace policy keeps namespaces off the tier
  (`runtimes`) or limits cordon kinds (`isolateCordons`).
- **The kill switch.** `legion operator isolate disable [--namespace N]`
  sets a replicated flag that workers read on the heartbeat: new
  invocations are refused with `isolate_disabled` (retryable), or burst
  when `burst.on` includes `"requested"`, and running hosts drain within
  `limits.wall`. `enable` reverses it. This is the CVE response.
- **Engine upgrades.** The compiled-module cache is keyed by engine
  version, so a new `legion-isolate` recompiles on first use per node;
  Phase 1 adds a prefetch that compiles a job's module for the new version
  before the old host drains. Hibernation files record the engine version
  and image digest; a mismatch wakes the isolate fresh with an `onResume`
  notice rather than restoring incompatible memory.
- **Breaking changes.** None; there is no installed base of isolates.

## 14. Open questions

1. **StarlingMonkey or QuickJS for the default `js` image?** Lean:
   StarlingMonkey for Web API compatibility and weval's speed, with QuickJS
   as `js-small` if Phase 0 shows its idle footprint is several times
   smaller. Phase 0 decides.
2. **Shared pool envelopes for many small jobs.** A FaaS platform with one
   Legion job per customer cannot afford an envelope per job per node.
   Lean: a namespace-level isolate envelope that every isolate task in the
   namespace draws from, with per-job quota from namespace policy; its own
   plan after Phase 2.
3. **UDS framing or a shared-memory ring between host and worker?** Lean:
   HTTP/2 over a socketpair first; a ring only if the worker's CPU per
   egress request exceeds 20 µs at the target rate.
4. **Python packages with native code.** Lean: curated images
   (`python-data` with numpy and pandas built for WASI) when Phase 3 finds
   working builds, and burst for everything else; never a package
   installer in the isolate.
5. **Should session isolates ever appear in the managers' index?** Lean:
   no; revisit only if a platform needs cluster-wide listing and can name
   the count.
6. **Hyperlight micro-VMs as the cordon boundary instead of a microVM per
   cordon.** Lean: revisit at Phase 6 if Hyperlight's Linux/KVM support
   and host-call model are stable.
7. **Per-tenant memory enforcement inside a shared host.** The kernel's
   memory controller is per process, so per-job memory inside a shared
   host is enforced by the host's own accounting, with the cordon's cgroup
   as the backstop. Lean: accept, state it in the public doc, and keep
   `cordon = "job"` for jobs that need the kernel to enforce their
   envelope.
8. **When does aarch64 join?** The 2026 critical escapes were
   aarch64-only or Winch-only, and OSS-Fuzz has no aarch64 runners. Lean:
   x86-64 only until continuous fuzzing covers aarch64 Cranelift and
   Phase 0's numbers hold on Graviton; aarch64 hosts meanwhile run the
   isolate tier only with `cordon = "job"`, or not at all.

## 15. Acceptance criteria

- **Phase 0:** a findings doc with the measurements listed in §11, and §4
  and §10 rewritten to them, with a calls entry for any target dropped.
- **Phase 1:** `legion isolate run` of a Rust component returns in under
  2 ms end to end on one node; `isolate_bench` shows instantiation at or
  under the Phase 0 target; an outbound request carries an injected
  credential the guest never read; every limit has its error envelope;
  `docs/task-runtime-isolate.md` exists and stands alone.
- **Phase 2:** two jobs in the same namespace share one host process and
  two namespaces do not; the confinement test kills the host on each
  forbidden syscall; `isolate disable` refuses new runs within one
  heartbeat.
- **Phase 3:** `isolate run --file x.js` and `--file x.py` meet the §10
  start targets; a dynamic Wasm module runs on its first call through the
  baseline tier and later calls use the optimized artifact.
- **Phase 4:** a node holds the §10 resident and hibernated counts in the
  density campaign; a hibernated session isolate wakes within target and
  keeps its state; an isolate awaiting a 20 s upstream holds no thread
  (shard thread count constant under 10^4 concurrent awaits).
- **Phase 5:** `vm.acquire` returns a machine that runs `exec`; a snippet
  calling a subprocess replays in a VM when it made no egress and fails
  with `isolate_unsupported` when it did; a burst VM reaches the same
  upstream with the same injected credential.
- **Phase 6:** slots per host with MPK striping at least 5× the guard-page
  layout's, measured; `cordonBoundary = "vm"` runs a cordon host inside a
  microVM end to end.
