# Judgement Calls: isolate-runtime-plan

Decisions made while designing and building
[`plans/isolate-runtime-plan.md`](../plans/isolate-runtime-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.** The doc holds two kinds of entry, ranked together:

- **Design-time calls** (2026-10-28 to 2026-11-07), made before any code.
  The research behind them is cited in the plan's §2 and §6. Entries 6 and
  17 revise the first draft after review the same day, and entry 18
  records what moved into shared plans on 2026-11-07; each says what it
  replaced. Entries 6, 12 and 18 rest on the companion plans' calls:
  [child sessions](child-sessions-plan_calls.md),
  [resize](resize-plan_calls.md), and
  [session runs](session-runs-plan_calls.md).
- **Implementation calls** (2026-11-11), made while building Phases 1 to 5
  and part of 6. They are inserted at their rank, not appended. Where the
  build contradicts a design-time entry, that entry keeps its text and gains
  a dated **As built** paragraph naming the entry that won; entry 7
  reverses entry 8 outright, and entries 9 and 21 replace most of entry 20.
  The measurements they cite are container numbers from a 4-vCPU
  Firecracker guest
  ([Phase 0 findings](../isolate-runtime-phase0-findings.md),
  `crates/isolate/images/README.md`), not the plan's AWS rig, which is
  still open.

The numbering changed on 2026-11-11 when the implementation calls were
merged. Old design-time numbers map to new ones as 1→1, 2→2, 3→4, 4→3,
5→6, 6→12, 7→23, 8→18, 9→8, 10→24, 11→11, 12→15, 13→13, 14→16, 15→20,
16→25, 17→17, 18→26, 19→32, 20→33, 21→45.

## 1. The engine is wasmtime; JS and Python run as interpreters inside Wasm, not on V8

**Call.** Isolates are WebAssembly components on wasmtime (pooling
allocator, copy-on-write images, Cranelift ahead of time). JavaScript runs
as SpiderMonkey compiled to Wasm (StarlingMonkey, with weval), Python as
CPython for WASI, both pre-initialized. V8 is not embedded.

```nickel
config = { image = "js", entry = "handler.js" }   # SpiderMonkey-in-Wasm, not V8
```

**Alternatives.**
- (a) V8 isolates, Cloudflare's design: the only engine with a production
  record of thousands of tenants per process, native async through
  promises, and a JIT. Embeddable through the `v8` crate or `deno_core`.
- (b) Both engines from the start, V8 for JS and wasmtime for everything
  else.
- (c) Hyperlight micro-VMs, for a hardware boundary per tenant.

**Trade-off.** JS throughput. StarlingMonkey in Wasm scores about 1/75 of
V8's JIT on Octane before weval, which buys back 2 to 3×. CPU-bound JS is
much slower in an isolate than it would be on V8. Accepted, with two
answers: CPU-bound JS runs with `tier = "vm"`, on a VM running Node with
V8's JIT behind a hardware boundary (the reply's `nextTier` says so after
an isolate run hits its CPU limit), and Phase 7 adds V8 as a second engine
if the trigger below is met.

**Why (performance at the density end, simplicity, security).**
- **Start:** 5 µs to a fresh Wasm instance against 0.7 to 5 ms for a V8
  isolate from a snapshot. That is what makes an instance per request
  affordable, and so what makes the safe lifetime the default (entry 16).
- **Density:** an idle instance costs only the pages it wrote; a V8
  isolate's floor is megabytes before user code, and V8's 4 GB shared cage
  caps the process. Lifting the cage (IsolateGroups) is unsupported
  upstream and unreleased in the Rust crate.
- **Containment:** a JS or Python engine bug, the class behind about half
  of Chrome's in-the-wild 0-days, corrupts one isolate's linear memory
  instead of the shared process. Google's own advice to high-risk users is
  to turn V8's optimizer off; this design never has one in the process.
- **Fit:** wasmtime's runtime-only build is about 2 MB of Rust; V8 roughly
  doubles the binary and adds a C++ build.
- Against (c): Hyperlight measured 9.1 ms cold for its Wasm guest, costs
  megabytes per VM, takes a VM exit per host call (two on nested
  virtualization), and is pre-1.0. It is watched as a cordon boundary
  (plan §14 Q6), not as the isolate tier.

**Revisit (the Phase 7 trigger).** Build V8 when, over a month of Phase 3
telemetry, more than 20% of JS CPU-seconds come from invocations that
spend over 10 ms of CPU and those tenants have not moved to `tier = "vm"`, *and*
upstream supports more than one pointer-compression cage per process.

**As built (2026-11-11).** wasmtime 50.0.1, pinned (`=50.0.1`), linked only
by `legion-isolate`. Phase 0 measured V8 closer than the table above
suggests: 0.92 ms to a first result and 1.2 MiB idle per isolate (without
pointer compression), against 0.45 ms and 0.65 MiB for StarlingMonkey in the
spike; V8's advantage is JIT throughput, which was not measured. The
trigger stands. weval is not used in the shipped `js` image (entry 32).

---

## 2. Tenants share a process within a cordon; the cordon is the trust unit, and no secret enters it

**Call.** One `legion-isolate` host process serves every isolate task in
a **cordon**: by default one per namespace. `cordon = "job"` gives a job
its own host; `cordon = "shared"` mixes namespaces, only where namespace
policy allows. Credentials never reach the host: the worker injects them
on egress.

```nickel
config = { cordon = "namespace" }   # default | "job" | "shared"
```

```text
[isolate] code-runner/run: config.cordon = "shared" is refused by namespace policy 'tenants' (isolateCordons = ["namespace", "job"])
```

**Alternatives.**
- (a) One host per job, the Phase 1 shape: simple, and the kernel
  enforces each job's envelope.
- (b) One host per node for everyone, the densest.
- (c) A cordon per tenant key inside a job.

**Trade-off.** Tenants in one cordon are not safe from a determined
Spectre attacker: researchers leaked a JWT across tenants in Cloudflare's
production Workers in 2026 at up to 12 bit/s. Per-job memory inside a
shared host is enforced by the host's accounting, not the kernel. And a
platform job with 10^4 small Legion jobs (one per customer) gets no density
from this, because each job still needs an envelope (plan §14 Q2).

**Why (performance, with security setting the boundary).** "Many tenants
per process" is the requirement, and (a) defeats it for any platform with
many jobs. (b) puts a free-tier tenant beside one handling sensitive data,
which Cloudflare refuses to do and which Google says only process
separation fixes. The namespace is the boundary Legion already uses for
ACLs, quotas, and policy, so an admin reasons about one unit. Keeping
secrets out of the process means the worst cross-tenant outcome is reading
another tenant's in-cordon data, never a credential; the public doc states
exactly that.

**As built (2026-11-11).** As designed. Cordon names are the job's
namespace, `job:<job id>`, and `shared`; a cordon runs one host process
per replica per node (entry 39). The policy rules are entry 43.

---

## 3. The host is a separate process and binary with no network; the worker does all I/O

**Call.** Isolates run in `legion-isolate`, a binary shipped beside the
worker (like `legion-guest`), in host processes the worker starts and
confines: an empty network namespace, a seccomp allowlist, Landlock
read-only on the artifact directory, no `execve`. Every outbound request
goes over a socketpair to the worker, tagged with PROXY v2 TLVs naming the
isolate, and the worker applies the job's `connect` policy and credential
injection. The worker binary does not gain wasmtime; network plugins stay
on wasmi.

**Alternatives.**
- (a) Isolates inside the worker process: no IPC, and runtime-only
  wasmtime is only about 2 MB.
- (b) The host does its own egress under policy pushed from the worker:
  one hop fewer.
- (c) The host role inside the main `legion` binary, compiler separate.

**Trade-off.** A local hop of roughly 10 to 30 µs and worker CPU per
outbound request; another binary to ship; the worker supervises more
processes.

**Why (security first, then simplicity).** (a) makes an engine escape a
worker compromise: every allocation's control and the node's credentials.
(b) puts credentials and network access into the multi-tenant process,
the two things the design keeps out of it (entry 2). A separate binary
keeps [mesh-interfaces entry 22](mesh-interfaces-plan_calls.md#22-the-plugin-engine-is-wasmi-an-interpreter-not-wasmtime)'s
reasoning intact for the worker, which still grows by nothing, while the
isolate host is exactly the case that entry said would justify wasmtime:
compute-heavy guests where an interpreter's 10× matters. Against (c): it
maps the whole worker into every multi-tenant process and couples the
host's upgrade to the worker's, and a fast engine patch (entry 2's
mitigation for an engine CVE) wants the host upgradeable on its own.

**As built (2026-11-11).** The split is as designed: `crates/isolate`
(`legion-isolate`, subcommands `host`, `compile`, `engine-info`) is the
only crate that links wasmtime, depends on `legion-proto` and nothing else
of Legion's, and is not a default workspace member. The host confines
*itself* (entry 14), and its network namespace holds an unconfigured
loopback that is down rather than nothing. The channel is not PROXY v2:
it is gRPC over two inherited socketpairs, attributed by an `Origin` on
every request (entry 10). The host has no HTTP or TLS client at all; the
worker originates TLS for `https://` (entry 37).

---

## 4. Isolates ride session mode and are invisible to raft and to the managers' index

**Call.** An `isolate` task must be `stateful.mode = "session"`. It gets
an envelope per node like any session task; the session router or an edge
caller picks the node; session isolates carry the node tag in their id.
Nothing per isolate or per invocation is written to raft or kept in the
managers' session index. Managers see one aggregate per task per node on
the heartbeat.

```console
$ legion isolate stats code-runner -j
{"ok":true,"data":{"task":"run","nodes":[{"node":"w-17","resident":48213,"active":312,
 "pagedOut":20114,"hibernated":391022,"invocationsPerSec":8410, ...}]}}
```

**Alternatives.**
- (a) A new placement model for isolates (a "function" object, a
  dedicated router).
- (b) Report session isolates in the managers' index like sandbox
  sessions, so `instance list` shows them.

**Trade-off.** No cluster-wide list of isolates (their child VMs are sandbox
sessions and are listed like any, with their owner reference); `legion isolate get <id>`
asks the node the id names. A node that dies loses its `local` sessions'
checkpoints, as for any `local` session.

**Why (simplicity, performance).** Session mode was built for "a tool
execution ... or a function invocation" and already has envelopes, routing
by id, edge starts with offline tickets, idle suspend, and manifests. A
new model would duplicate all of it. Against (b): the index costs about
200 bytes per running session per manager; 10^5 isolates on each of 100
nodes is 2 GB per manager, for a list nobody reads at that size.

**As built (2026-11-11).** As designed, and the node side goes further
than the plan said: isolates do not use the worker's per-session claims
either (entry 5). The heartbeat carries `isolate_tasks` and
`isolate_hosts`, held in memory per manager (entry 56). Session isolates
are reached by their id's node tag through the existing probe; an id
derived from an idempotency key has no tag and needs `--on-node`
(entry 30). Every session in this runtime is `local`: `durability =
"store"` is not built (entry 44).

---

## 5. Isolates bypass the worker's per-session claim machinery: a supervisor owns them

**Call.** An isolate run or a session-isolate start never creates a
session claim, an allocation payload, or a launch slot. A worker-side
`IsolateSupervisor` (`crates/worker/src/worker/isolates/`) owns the cordon
hosts, compiled modules, prepared tasks, session isolates, the tier
decision, stats, and the `IsolateWorker` service the hosts call. The
`isolate` `TaskPlugin` exists only for the submit-time surface (schema,
validation, normalization); its `start`/`stop` refuse, it is not
host-level (no `exec` into an isolate), and its `resource_enforcement` is
`Unsupported` because the task is never an allocation. The supervisor
finds its work by reconciling the node's isolate envelopes
(`WorkerTaskManager::isolate_envelopes()`) every second, and on demand
when a run, start, or ingress call names a task it does not hold.

**Alternatives.** (a) The path every other runtime takes:
`admit_and_launch`, `TaskPlugin::start`, and the exec-based session runs.
(b) Hooks at envelope start, stop, and resize instead of a reconcile.

**Trade-off.** A second session lifecycle on the node. Anything built on
claims (the claim records, `SessionReport`, the index, `instance list`)
does not see isolates, and the session verbs reach a session isolate
only because the worker synthesizes a `SessionStatus` for it (entry 30).
A new or resized envelope is noticed up to a second late unless a call
arrives first.

**Why (performance, simplicity).** A claim costs a record, a payload,
and a launch slot per unit, and a node holds up to 10^5 isolates whose
starts take microseconds. The reconcile is one read-only method in
`task_manager.rs`, so no other runtime's behavior can change, and it
covers restores, job updates (the spec fingerprint changes and the task
is prepared again), resizes, and departures in one place.

---

## 6. One run, one place: the tier is chosen before a run starts, and Legion never re-runs one (revised)

**Call.** Every run happens in exactly one place. A session run's `tier` is
`isolate`, `vm`, or `auto` (the default), and `auto` decides *before*
anything runs, from a pre-flight import check and a per-node memo of code
that needed a VM; a caller that kept a previous reply's `nextTier` asks
for that tier. A run stopped in an isolate by an
unsupported call or a limit stays stopped; its reply carries `nextTier =
"vm"`. A caller who wants the rerun asks for it (`--fallback vm`), and the
reply says `ranTwice`.

```console
$ legion session run code-runner --task run --file transcode.py -j
{"ok":true,"data":{"tier":"isolate","stopped":{"reason":"unsupported",
 "detail":"subprocess.run: processes are not available in an isolate", ...},"nextTier":"vm", ...}}
```

**This reverses the first draft.** The plan as first written (2026-10-28)
had Legion replay a failed run in a VM automatically, but only if the run
had made no outbound request yet (`burst.on = ["unsupported", "limits"]`,
`replay = "before-egress"`). Review the same day found it confusing:

- whether a snippet burst or failed depended on the order of two lines (an
  API call then a subprocess failed; the reverse silently re-ran);
- one call could run in two different Pythons, the isolate's and the VM's;
- the invocation's size and accounting changed in the middle of the call.

**Alternatives.** (a) The draft's replay before egress. (b) Replay always,
leaving idempotency to the author. (c) Continuation migration (Phase 8).

**Trade-off.** Code the pre-flight check misses runs in the isolate first
and stops; the caller pays one round trip to rerun it. The memo and
`nextTier` bound that to once per code hash.

**Why (ease of reasoning).** "Where did my code run, and how many times" has
one answer per call, visible in the reply, with no rule that depends on
what the code did first. A rerun is the caller's decision, so the caller,
who knows whether the code is safe to run twice, makes it.

**As built (2026-11-11).** As designed. The decision is node-side, in this
order: an explicit `tier`; else `auto` goes to the VM when the isolate
runtime is disabled for the job and the task has a VM task (entry 31),
when the pre-flight scan (image tasks only, a heuristic) finds a module the
image lacks, or when the node's memo holds the code hash within
`config.vm.memoFor`; else the isolate. `--fallback vm` is a second request
from the CLI or the MCP tool, sent only when the first reply has `tier =
"isolate"`, a stop of `unsupported` or `limits`, and `nextTier = "vm"`; it
reuses the idempotency key as `<key>:fallback`, and is refused with
`--session` (a VM run is not in the session isolate) and with `--dry-run`:

```text
tierReason: "fallback: stopped in the isolate, unsupported (subprocess.run)"
```

---

## 7. Every guest runs on a fiber; there is no stackless WASI 0.3 world (reverses entry 8)

**Call.** The host calls every guest with `call_async` on a fiber with a
small stack (`async_stack_bytes`, default 128 KiB plus a guard page), and
builds the engine with `concurrency_support(false)`, which is part of the
artifact compatibility key. wasmtime-wasi and wasmtime-wasi-http are built
with `p2` only (the default `p3` would pull component-model-async back
in). The guest world is WASI 0.2; there is no WASI 0.3 form of it. A
session isolate can hibernate only when it has no call in flight, and
`total_stacks` bounds how many resident isolates can have made an async
call.

**Alternatives.** Entry 8's design: the Component Model async callback ABI
as the target, so a waiting guest holds no stack and can hibernate at any
await, with fibers as the fallback for WASI 0.2 guests. Supporting both
paths.

**Trade-off.** Goal G3's "an isolate awaiting I/O holds no stack" is not
met: an isolate awaiting a 20 s upstream keeps its fiber stack and its
slot, and cannot hibernate until the call returns. Revisit when wasmtime
releases a store's fiber stack between calls; the stackless world then
becomes worth building (open TODO item).

**Why (performance, simplicity).** Measured on wasmtime 50.0.1 in Phase 0
(findings items 7 and 8): every `Store` that has run an async call keeps a
fiber stack until it is dropped, *on the callback ABI too*, and there is no
public API to release it. So the stackless path does not deliver the
property it was chosen for, and it costs more: an await is 1.8 µs through
the callback ABI against 0.48 µs on a fiber, a trivial call 2.9 µs
through `run_concurrent` against 0.18 µs through `call_async`, and
`concurrency_support` adds about 0.7 µs to every call. One async path is
also one fewer to test, and it keeps the engine's newest code path (the
one behind 2026's guest-controlled stack write) out of the host.

---

## 8. Waiting holds nothing: a host-driven event loop on the stackless callback ABI, fibers as the fallback

**Call.** The host drives each guest's event loop. Guests on the WASI 0.3
Component Model async callback ABI run a turn, return to the host when
they await, and hold no native stack between turns. WASI 0.2 guests that
block in sync imports run on a fiber per in-flight call (128 KiB stacks,
pooled).

**Alternatives.**
- (a) Fibers for everyone, wasmtime's mature async model.
- (b) Wait for Wasm stack switching.
- (c) Asyncify-style transforms.

**Trade-off.** The newest code path in the engine: WASI 0.3 shipped
2026-06-11, and a critical guest-controlled write to the host's native
stack in `component-model-async` was fixed on 2026-10-02
([CVE-2026-115384](https://github.com/bytecodealliance/wasmtime/security/advisories/GHSA-32h6-97mm-8q3c)).
The compiler process validating every component with the newest
`wasmparser` (entry 13) is the published mitigation and is applied to all
modules. Two async paths to test and support.

**Why (performance at density).** With fibers, an isolate awaiting a 20 s
LLM call pins a slot and a stack, and 10^5 suspended fibers are about 12.5
GiB of stack address space. With callbacks the isolate is data between
turns, which is also what lets it hibernate *while it waits* (entry 20).
(b) is a Phase 3 proposal with no tiered wasmtime support; (c) costs code
size and speed on every call.

**As built (2026-11-11): reversed by entry 7.** Phase 0 found that on
wasmtime 50.0.1 a store keeps a fiber stack after any async call on the
callback ABI as well, so (a) shipped. The reasoning above still says what
to build once the engine releases that stack.

---

## 9. Hibernation is a state checkpoint, not a memory image

**Call.** A hibernated session isolate is its `legion:isolate/state` map
plus its session metadata, serialized by the host (format "LISC" v1,
`crates/isolate/src/checkpoint.rs`) and stored by the worker in the node
content store (`snapshots` class, pinned to `isolate-session:<id>`,
dropped on stop or after `suspend.keepFor`). Waking makes a fresh instance
with `control.info().resumed = true`; linear memory does not survive. A
checkpoint written by another engine version or another module is still
restored at the state level. A trap or limit stop in a session or tenant
isolate drops the (poisoned) instance but keeps the state, and the next
call gets a fresh instance with `resumed = true`. Idle tenant instances
are dropped, not checkpointed.

```python
import legion
if legion.info()["resumed"]:
    ...   # memory is fresh; what you kept is in legion.state
legion.state.set("cursor", str(cursor))
```

**Alternatives.** (a) The plan's memory image: private pages diffed
against the artifact, plus globals and tables (§6.7). (b) Legion's own
instrumentation pass, or an upstream snapshot API. (c) Refuse a
checkpoint from another engine or module version and start the session
empty.

**Trade-off.** A guest must keep what matters in `state`: globals, caches,
a JS heap, a Python `__main__` namespace, and open handles are gone after
hibernation, a limit stop, or a trap. A guest whose code changed sees
state written by the old code and versions its own keys; a guest that
corrupted its state keeps the corruption.

**Why (performance, durability).** The only public way to read and write a
component instance's memories and globals in wasmtime 50 is
`guest_debug(true)`, which costs 320 µs per instantiation (against 8.5 µs)
and doubles run time, and it still loses the component handle tables
(findings item 13). The state map is engine-independent, so refusing it
across versions would silently empty every session on each engine upgrade.

---

## 10. The host protocol is gRPC over two inherited socketpairs, with an `Origin` on every request

**Call.** The worker starts each host with two socketpair fds
(`--control-fd`: the host serves `IsolateHost`; `--worker-fd`: the worker
serves `IsolateWorker`) and `--config-fd` carrying `HostConfig` as JSON,
all defined in `proto/isolate_host.proto` with a `PROTOCOL_VERSION`. Every
call from the host names an `Origin` (job, task, isolate id, tenant),
which the worker checks against the tasks of that host's cordon before it
acts. The host serves exactly one connection on its control socket and
exits when it closes. Per-isolate attribution of egress is debug-level log
fields (`job_id`, `task`, `isolate_id`, `tenant`, `allocation_id`); the
datapath records denials against the envelope allocation, which names the
isolate task (entry 19).

**Alternatives.** (a) The plan's HTTP/2 with PROXY v2 TLVs naming the
isolate (§6.9). (b) The apps plan's workload channel (entry 18's last
row). (c) A shared-memory ring (§14 Q3). (d) A new PROXY TLV on the
connection to each datapath listener.

**Trade-off.** `legion net denials` shows the task, not which isolate or
tenant was denied. The protocol is a second, internal gRPC surface to
version (node-local, so only the worker and the host it ships with must
agree). No measurement yet says whether the worker's CPU per egress
request needs a ring.

**Why (security, simplicity).** The `Origin` check means a compromised
host can speak only for its own cordon's tasks. None of the datapath's
listeners reads a PROXY header from its client today; teaching every
listener to trust one from a workload is a security-sensitive change a
gated workload could try to forge, for attribution nobody had asked to see
yet. tonic is already in both binaries, and one connection for the host's
life means it never outlives its worker, with no polling.

---

## 11. A new runtime, `isolate`, that bursts to `sandbox`, not a mode of the `sandbox` runtime

**Call.** The request was "a sandbox runtime with two modes". The plan
ships it as two runtimes that act as one product: `runtime = "isolate"`,
whose `config.vm` names a sibling task on the unchanged `sandbox` runtime.

```nickel
{ name = "run", runtime = "isolate", config = { image = "python", vm = { task = "machine" } }, ... },
{ name = "machine", runtime = "sandbox", config = { image = "python:3.13-slim" }, ... },
```

**Alternatives.** `runtime = "sandbox"` with `config.mode = "isolate" |
"vm"`, which matches the request's words.

**Trade-off.** Users meet two runtime names for one idea, and the docs
must present them as one tiered system.

**Why (simplicity of the surface and the code).** The two modes share no
config fields beyond `env`: one has `image`, `kernel`, `vcpus`, and block
volumes; the other has `module`, `limits`, `lifetime`, and `cordon`. A mode
switch would make every field conditional on another. `TaskPlugin`
declares capabilities per runtime, not per config (`supports_signal`,
`file_delivery_root`, live restart), and the answers differ. Namespace
policy's `runtimes` allowlist can then admit isolates without VMs or the
reverse.

---

## 12. Burst is composed from child sessions and a declared VM task, not from burst machinery or vertical scaling

**Call.** The VM is an ordinary sibling session task the job declares
(`runtime = "sandbox"`), listed in the isolate task's `children`. A VM run is a
session run on it; an isolate that wants to drive a machine starts
one as a [child session](child-sessions-plan_calls.md) with `vm.acquire()`.
Growing either is a [resize](resize-plan_calls.md). Nothing is generated,
and there is no `burst` block.

```nickel
config = { image = "python", vm = { task = "machine" } },
children = { tasks = ["machine"] },
```

**Alternatives.** (a) The first draft: a `burst` block from which submit
generated a hidden `<task>-burst` task with a nested envelope, a trigger
list, and a replay policy. (b) Burst as vertical scaling: an isolate that
grows past a threshold becomes a VM.

**Trade-off.** Two tasks to write instead of one block, and the shared
network rules need a Nickel binding to avoid repeating them.

**Why (composability, simplicity).** Each piece is useful without burst:
child sessions serve an `exec` agent spawning tool sandboxes, resize serves
the autoscaler, and the VM task is a session task like any other, so
accounting at burst is "a second unit appears in its own envelope", never
"a unit changes size". (b) would weld capability to size: most bursts are
for a subprocess or a native package, not memory, and an isolate that only
needs memory gets a higher ceiling in microseconds.

**As built (2026-11-11).** Built as designed, with two differences a user
sees. The VM tier exists only when `config.vm` is present: `vm.task`
defaults to the single entry of `children.tasks` only inside the block, so
the common case is `vm = {}`, and a task whose `children` list exec
children an isolate drives is not mistaken for one with a VM tier
(`vm = {}` with two children is refused: "there is no default"). And the
"shared network rules" cannot be shared yet: a sandbox session task still
refuses `connect` (entry 50). A VM run's `args` are appended to the VM
task's own `run.command` (entry 27).

---

## 13. Code is compiled by a node-local compiler process with Cranelift only, x86-64 first; user `.cwasm` is never loaded

**Call.** `legion-isolate compile` runs as a short-lived, confined process
on the node and writes artifacts into the local content store, keyed by
module digest, engine version, and CPU features. The host role never
compiles. Cranelift is the only compiler; Winch and Pulley are off. x86-64
is the first supported host architecture.

**Alternatives.**
- (a) Compile once per cluster (a manager or a `legion-system` app) and
  distribute signed artifacts.
- (b) Winch for fast first compiles of dynamic modules.
- (c) wasmi beside wasmtime for first runs, no compile at all.

**Trade-off.** Every node compiles each module once, and a dynamic Wasm
module pays a Cranelift compile on its first call. aarch64 nodes wait.

**Why (security).** Deserializing a `.cwasm` is arbitrary code execution
by design, so the artifact must never cross a trust boundary; with (a) a
signature check is all that stands between a bucket writer and every node.
Winch is incompatible with epoch interruption and had a 2026 sandbox
escape. (c) puts two engines' bugs in one trust boundary for a rare path:
dynamic code is mostly JS and Python source, which needs no compile.
aarch64 had both April 2026 critical escapes and no continuous fuzzing.

**As built (2026-11-11).** As designed, x86-64 Linux only. The artifact is
derived content of kind `cwasm`, keyed by the module's digest and the
`compatKey` that `legion-isolate engine-info` reports (engine version,
engine configuration including the slot layout, Spectre setting and
`concurrency_support`, and CPU features). The compiler validates with
`wasmparser` first, refuses a core module rather than wrapping it, and
refuses an import the host does not provide by name: first against the
host's list of provided interfaces with semver matching (wasmtime alone
accepts an import of an *empty* instance it has no definition for), then
with the host's real linker. It runs with `RLIMIT_AS` = 16 GiB and a 300 s
timeout; its own confinement is best effort and has no seccomp filter. A
component the content store cannot keep (no derived budget or no room) is
compiled into the worker's `<state>/isolates/cwasm/<key>/`. The host does
not compare keys when it loads an artifact: wasmtime's own
`deserialize_file` check refuses one whose code-generation settings differ
(answered as `FAILED_PRECONDITION` with wasmtime's reason), and the
worker's keying keeps layouts apart. A dynamic Wasm
module sent as a session run is not built: runs execute source in an
image or a task's own module.

```text
this is a core WebAssembly module, not a component; build for wasm32-wasip2 or wrap it with `wasm-tools component new`
the component imports `wasi:nope/thing@0.2.0`, which the isolate host does not provide (it offers WASI 0.2 up to 0.2.12 without wasi:http/incoming-handler as an import, and legion:isolate@0.1.0 control, state, and vm)
```

---

## 14. The host confines itself before it starts a thread, and refuses to run when a layer is missing

**Call.** Before it starts any thread or loads any module, the host
enters a user namespace (unprivileged ids, every capability dropped), a
network namespace (an unconfigured loopback, down), and mount, IPC and
UTS namespaces; sets `no_new_privs`; restricts itself with Landlock to
read-only access to its artifact directories; and installs a seccomp
allowlist whose default action kills the process. It reports what applied
in its `Hello`. The worker's `isolate.confine` decides what a missing
layer means:

```nickel
isolate = { confine = "enforce" }   # default: every layer or the host exits | "best-effort" | "off"
```

The allowlist was found by tracing the host's threads under `strace -ff`
in the spawn tests and trimmed; it permits `socketpair` for `AF_UNIX` only
(tokio's signal driver makes one), and `LEGION_ISOLATE_SECCOMP=log` turns
kills into log entries for diagnosing a missing call. Anonymous executable
memory is not denied: seccomp cannot tell an anonymous `mprotect` from a
file one, so it needs an LSM (SELinux `execmem`), which is not built.

**Alternatives.** (a) The worker confines the child between `fork` and
`exec`. (b) `best-effort` as the default. (c) A separate tokio for the host
built without the `signal` feature, so no `socketpair` at all.

**Trade-off.** An allowlist kills the host on a syscall path the tests
did not reach (a guest growing memory with `mremap` was added by reading,
not observed); the kill shows as the host exiting with SIGSYS, and the
worker restarts it (entry 53). A compromised host can make connected Unix
pairs, which reach nothing outside the process. The "no new code" goal
(G6) holds only as far as seccomp can take it.

**Why (security).** With `enforce` by default, a node that cannot confine
a host does not run isolates at all rather than running them silently
weaker. Self-confinement keeps every layer in one binary that a test build
(`--features confinement-test`) can probe from inside; all five layers
apply in the container this was built in. One tokio keeps the workspace's
dependency graph whole for the cost of one argument-filtered syscall.

---

## 15. Slots use guard pages by default; MPK striping is Phase 6; the layout belongs to the cordon, not the job

**Call.** The pooling allocator uses wasmtime's default guard-page layout
(about 16,000 to 32,000 slots per process); a cordon that needs more runs
more host processes. MPK striping (ColorGuard, about 15× more slots) is
built in Phase 6 behind its own soak. Small reservations with explicit
bounds checks are an operator density mode for `shared` cordons only. No
job can pick its layout.

**Alternatives.**
- (a) MPK striping from Phase 1, the obvious density win, and per the
  paper also a speculative-access barrier between neighbours.
- (b) Small reservations with explicit bounds checks as the default:
  millions of slots.

**Trade-off.** Fewer isolates per process until Phase 6, and more
processes per cordon meanwhile (ColorGuard measured one MPK process about
29% faster than 15 processes).

**Why (security over density, for now).** The density-tuned layouts are
the least-tested paths: wasmtime marks MPK experimental, and a 2026 leak
between pooled slots appears only with zero guard and a reservation under
4 GiB. Guard pages make heap accesses Spectre-safe by construction with no
run-time cost; explicit checks cost 1.6 to 1.8× with Spectre guards. MPK's
protection also assumes no control-flow hijack (a guest that can jump to a
`wrpkru` gadget disables it), the same caveat Cloudflare's threat model
states. Making the layout a cordon property means one operator decision,
not a per-job dial nobody can audit.

**As built (2026-11-11).** Guard pages are the default. The other two
layouts exist ahead of their soak as the worker's `isolate.layout =
"guard-pages" | "mpk" | "small"`, for every cordon on the node (there is no
per-cordon setting yet), and were measured for slot counts only: 21,870,
328,842 (with `vm.max_map_count` raised), and 702,500 slots per process.
Two sharp edges: an `mpk` host on a CPU without usable protection keys
falls back to guard pages silently (`engine-info` reports `mpkAvailable`,
but the compatibility key still says `mpk`), and wasmtime splits an MPK
pool into up to 15 stripes, so a host with few slots can refuse an
instance before `slots` is reached. The soak, MPK as a default, and the
Spectre-mode knobs are open; the worker always builds hosts with Spectre
mitigations on.

---

## 16. The default lifetime is one instance per request

**Call.** `lifetime = "request"` by default: a fresh instance per
invocation, destroyed after. `"tenant"` (reused per tenant key while warm)
and `"session"` (addressable, hibernating) are opt-in.

**Alternatives.** Workers' model as the default: a long-lived isolate per
script, globals persisting across requests.

**Trade-off.** No in-memory caches across requests by default, and images
with costly per-call setup (Python) pay it every time unless the author
opts into `tenant`.

**Why (security, made affordable by entry 1).** An instance that lives
for one request cannot accumulate a side channel's bits across requests,
and a corrupted one dies with its request. Fastly's argument, and the 2026
Workers attack needed a connection held open for hours. At 5 to 30 µs per
instance the safe default costs almost nothing.

**As built (2026-11-11).** As designed. Reuse needs the guest to export
`legion:isolate/runner` (entry 28). For the language images the fresh
cost is milliseconds, not microseconds: hello world 1.14 ms (`js`) and
2.55 ms (`python`) fresh against 107 µs and 171 µs warm, so `tenant`
matters more for images than the "almost nothing" above suggests.

---

## 17. Isolates use `resources.memory` and `memoryMax`, not their own cap and charge (revised)

**Call.** An isolate's reservation is `resources.memory` (what the node
admits against the envelope: its expected private memory) and its ceiling
is `resources.memoryMax` (the cap on linear memory), the same two fields,
with the same meaning, as every other runtime. `memoryMax` defaults to 128
MiB; `memory` defaults to the image's measured idle size.

```nickel
resources = { cpu = 10, memory = 2, memoryMax = 128 },
```

**This revises the first draft,** which had `config.limits.memory` as the
cap and `resources.memory` as a separate "admission charge": a parallel pair
that meant the same thing as `memory`/`memoryMax` under different names.

**Alternatives.** (a) The draft's pair. (b) One number, charged at the cap.

**Trade-off.** `memoryMax` means "linear memory size" for an isolate and
"guest size" for a sandbox; both are the unit's addressable memory, but a
reader comparing runtimes has to know that.

**Why (simplicity, composability).** One pair of fields across runtimes is
what lets resize (which changes exactly these fields) and the scheduler
treat an isolate like any other unit. Against (b): charging the cap makes a
JS isolate that writes 2 MiB cost 128 MiB of envelope, 60× less density for
a ceiling nearly no isolate reaches.

**As built (2026-11-11).** As designed. `memoryMax` caps linear memory
*plus* the scratch filesystem's file bytes (entry 22). Defaults filled at
submit: `memory` is the image's measured idle size rounded up (`js` 1 MiB,
`python` 2 MiB, a module 1 MiB), never above `memoryMax`; `cpu` is 10 when
none of `cpu`/`cpus`/`cores` is set, because the cluster's
`taskResourceDefaults` are sized for processes and would fit about 100×
fewer isolates per envelope; `resources.cores` is refused. The "measured
private-pages estimate reported beside the reservation" is `memoryBytes`
next to `reservationBytes` in `legion isolate stats`.

---

## 18. What is not isolate-specific is planned as shared primitives (revised)

*Revised 2026-11-07.*

**Call.** Six things the first draft built inside the isolate runtime now
live in plans of their own, usable by every runtime, and the isolate plan
uses them:

| Was in the isolate plan | Now |
| --- | --- |
| `RunIsolate`, a one-shot run with a structured reply | `RunSession` ([session-runs plan](../plans/session-runs-plan.md)) on any session task; the isolate adds `tier` |
| `legion operator isolate disable` | `legion operator runtime disable <runtime>` ([runtime-disable plan](../plans/runtime-disable-plan.md)) |
| A compiled-module cache and pre-initialization caches | the node content store's `derived` class ([derived-content plan](../plans/derived-content-plan.md)), with the root disk cache |
| Hibernation files under `<state>/isolates/` with their own budget | session checkpoints in the node content store, as for any suspended session |
| A new "hibernate isolates" memory-pressure step | the pressure response's existing `reclaim` and `suspend` steps, through a runtime reclaim hook |
| The isolate host's own channel to the worker | the apps plan's workload channel, with one attribution rule |

The stopped-run rule (a run stopped by a limit is data, not an error) was
entry 19 of this doc's first draft; it moved with runs and is
[session-runs calls entry 4](session-runs-plan_calls.md#4-a-stopped-run-is-data-error-envelopes-are-for-runs-that-never-ran).

**Alternatives.** (a) Keep them isolate-specific until a second runtime
asks. (b) Generalize more: the shard executor, cordons, and per-session
reporting as jobspec knobs.

**Trade-off.** More plans to keep in step, and the isolate plan depends on
four of them before its later phases ship.

**Why (simplicity, extensibility).** Each of the six already had a second
user or a near-duplicate in the code: sandbox users write
start-exec-stop wrappers; runc and VMM CVEs need the same kill switch;
`rootfs_cache.rs` is a derived-content cache; sessions already checkpoint
into the content store; the pressure response already reclaims; and the
apps plan already designs the socket. Building them twice is the expensive
path. Against (b): the shard executor, cordons, and an `aggregate`
reporting knob have no second user, so they stay inside the isolate
runtime (the reporting level is a runtime property, not a jobspec field)
until one appears.

**As built (2026-11-11).** Five of the six rows were used as planned:
`RunSession` with the isolate's fields, `runtime disable isolate`
(entry 31), derived content of kind `cwasm` (entry 13), checkpoints in the
content store's `snapshots` class (entry 9), and the pressure response's
`reclaim` and `suspend` steps through a `PressureSource::isolates` hook
(entry 21). The last row was not: the host speaks its own node-local
protocol, not the workload channel (entry 10). The pre-initialization
kinds (`preinit-image`, `preinit-entry`) are defined but have no builder
(entry 29).

---

## 19. An isolate task's datapath is bound to its envelope allocation, forced gated; `service` is refused

**Call.** At submit, the isolate task's `network` (mode forced to `gated`,
ports and egress/ingress policy kept) and `connect` are copied onto its
generated `<task>-envelope`, in the desugared form normalization produces,
so normalization stays idempotent; a `connect` with no `network` still
gets a gated network. On the node, an isolate envelope attaches as
`Attachment::Isolate { dir }` with Unix-socket listeners in
`<allocs>/<id>/.isolate` (`net.sock_<port>` for the egress proxy and each
upstream, `ingress.sock_<label>` served by the supervisor). The node's
identity for the envelope strips `-envelope`, so denials and access logs
name the task the job declares. An isolate task may declare ports, a
policy, and `connect`; it may not declare `service`, a `network.mode`
other than gated, or `connect.templates`.

```text
task run is an isolate task and declares a service. Isolates take requests on network.ports through their envelope, which registers no service
Hint: remove service and declare network.ports = [{ name = "http" }]
[isolate] job/run: network.mode = "host" does not apply: an isolate has no network of its own, and all its I/O goes through the node datapath (gated)
[isolate] job/run: connect.templates renders files into a task directory, which an isolate does not have
```

**Alternatives.** (a) Reuse `Attachment::GuestVsock` with a scratch dir
for the envelope. (b) Copy `service` too and register the envelope in the
catalog. (c) Leave the network on the isolate task and have the worker look
it up.

**Trade-off.** No catalog registration for isolate HTTP handlers; callers
reach them through ingress ports. One more `Attachment` variant, with an
arm in every match on it. The envelope's group in the identity string is a
guess when it was the generated one.

**Why (simplicity).** The envelope is the only allocation the node has for
the task, so binding it there needs no new datapath concept and every check
(gate, policy, handlers, host guard, credential injection, denial and
activity ledgers) applies unchanged. `GuestVsock` carries VM behavior that
is wrong here: a Firecracker `CONNECT <port>` handshake on ingress, a port
record read from a sandbox dir, and a resolver on every gated sandbox's
socket; an isolate has no guest agent and is addressed by port *label*.

---

## 20. Idle memory leaves in four tiers, and hibernation happens at turn boundaries

**Call.** Hot, then paged out (`MADV_PAGEOUT` to zswap/zram/swap) after
`pageOutAfter`, then hibernated (private pages diffed against the image,
plus host-side state, compressed into a session checkpoint in the node
content store; slot freed) after
`suspend.idle.after` or under slot or memory pressure, then evicted or
published per `suspend.keepFor` and `durability`. Host-held sockets stay
open across hibernation.

**Alternatives.** Cloudflare's model: LRU-evict whole isolates and let the
next request start fresh.

**Trade-off.** The paged-out tier depends on the node having compressed
swap or swap; without it, `MADV_PAGEOUT` does nothing for anonymous memory
and the tier collapses into hot. Checkpoints cost disk, from the
`snapshots` budget.

**Why (performance at density).** Eviction is fine for stateless
handlers, which already get `request` lifetime. For a million resident
agents it throws away exactly the state the platform persona wants kept.
Reusing session sleep's vocabulary (`idle.after`, `keepFor`, `durability`)
and its checkpoints keeps one model for both tiers (entry 18).

**As built (2026-11-11).** The four tiers exist, but two of the three
mechanisms differ. Paging out is the kernel reclaiming from the host's
cgroup, not `MADV_PAGEOUT` per isolate (entry 21); hibernation keeps the
`state` map, not memory pages (entry 9), and happens only when no call is
in flight (entry 7). Only `durability = "local"` exists (entry 44).
Host-held sockets are not built: bodies are buffered and there are no
WebSockets, so nothing stays open across hibernation.

---

## 21. Paging out is kernel reclaim on the host's cgroup, not `madvise` per isolate

**Call.** Every `isolate.pageOutAfter` the worker asks each host which
idle instances are cold (`Reclaim{PAGE_OUT}` returns their bytes and marks
them `paged_out` until they next run), then writes that many bytes to the
host cgroup's `memory.reclaim` and lets the kernel choose the pages. The
host never pages anything out itself (it ignores `page_out_after_millis`).
Paging is possible only where the host has a cgroup (resource enforcement
on, cgroup v2), its parent has `memory.reclaim`, and the node has swap or
zswap; the worker checks once at start and logs the reason once when it
cannot. The host's cgroup is
`/sys/fs/cgroup/<cgroupParent>/isolate-hosts/<cordon>-<replica>`, with
`memory.max` the sum of the cordon's envelopes (each replica gets the
whole sum: a ceiling, not a split) and `pids.max` 1024; the pid is moved in
right after spawn. Under memory pressure, the pressure response's
`reclaim` and `suspend` steps ask the hosts first (page out, then
hibernate) and only then touch allocations.

**Alternatives.** (a) The plan's `MADV_PAGEOUT` on each cold isolate's
memory. (b) No paged-out tier until an engine API exists.

**Trade-off.** The kernel picks the pages, from anything in the host, not
only the instances the host named. The "cannot page here" reason is only
in the worker log; `IsolateHostStats` has no field for it, and
`pagedOut` simply stays 0. The cgroup's `memory.max` is not resized when
an envelope grows or shrinks, no `cpu.weight` is set, and the host's first
instructions run in the worker's cgroup. None of this is exercised in the
container it was built in (no swap; cgroup placement runs only with
enforcement on).

**Why (simplicity).** Without `guest_debug` (entry 9) the host cannot
learn a component memory's address to `madvise` it. The cgroup the host
needs anyway as the cordon's backstop (plan §6.11) already exposes the
kernel's own reclaim, so no new mechanism was needed.

---

## 22. The scratch filesystem is Legion's own in-memory `wasi:filesystem`

**Call.** `crates/isolate/src/scratch/` implements `wasi:filesystem` over
an in-memory tree per isolate; every other WASI 0.2 interface is linked
one by one from wasmtime-wasi (sockets are linked, because the Python
image imports them, and deny everything). File bytes are charged by length
to the same account as linear memory, against `memoryMax`. The tree
follows POSIX for `..` (it may climb above a descriptor's directory, the
whole tree is the guest's), has no symbolic or hard links
(`not-permitted`), holds at most 16,384 nodes with 255-byte names, and its
host functions complete synchronously in memory. A refused write is
`insufficient-space`, but wasi-libc never asks a stream for its filesystem
error code and turns every stream failure into EIO:

```text
write() / f.write() past memoryMax   -> EIO (errno 29) in wasi-libc guests
ftruncate() / set_len() / pwrite()   -> ENOSPC (errno 51)
```

**Alternatives.** (a) wasmtime-wasi's filesystem over a host directory
per isolate, the only filesystem it ships. (b) An empty read-only preopen.
(c) cap-std semantics, where `..` above the base is an error.

**Trade-off.** About 35 trait methods Legion owns and must keep in step
with WASI. A full scratch disk reads as an I/O error from a plain write
(the run's memory stop is still classified correctly). Links are absent;
hard links would be cheap to add if a guest needs them.

**Why (security, simplicity).** (a) gives every isolate a writable host
filesystem, which G6 rules out, and (b) breaks both images, which import
`wasi:filesystem` and stage the run directory through it. The tree is one
isolate's private namespace, so there is nothing for `..` to escape to and
POSIX behavior is what guests expect. The EIO gap is wasi-libc's and is
the same under wasmtime-wasi's own filesystem.

---

## 23. A session's address is always its isolate; its VM is reached through it

**Call.** After a session isolate starts a child VM, requests still address
the isolate's session id; the isolate handles each one or forwards it to
its VM. Stateless runs have no address to keep: `nextTier` makes the caller
the memory across nodes. Bulk streams get connection handoff later
(child-sessions plan, Phase 3).

**Alternatives.** Re-point the session's address at the VM after a burst.

**Trade-off.** An extra local hop through the isolate per forwarded request
(tens of microseconds), and a network hop when the VM is on another node.

**Why (simplicity).** The full argument is
[child-sessions calls entry 4](child-sessions-plan_calls.md#4-the-owners-id-is-the-only-address-children-are-reached-through-it):
re-pointing needs a mutable mapping, two kinds of address, an in-flight
window, and a VM's wake time on every request, while the isolate costs
almost nothing to keep in front.

**As built (2026-11-11).** As designed: the isolate forwards with
`machine.fetch` or `exec`, and child VMs always start on the isolate's own
node. Connection handoff shipped for other owners (child-sessions Phase
4), but not from an isolate: ingress bodies are buffered, so bulk traffic
to a child goes through the isolate.

---

## 24. Isolate sessions are never promoted to keyed instances

**Call.** `legion instance promote` on an isolate session answers
`FAILED_PRECONDITION`. A durable, named isolate uses what session
durability already gives: an id derived from an idempotency key, and
`suspend.durability = "store"`, which publishes its checkpoints with a
manifest so it survives its node.

```text
FAILED_PRECONDITION: isolate sessions cannot be promoted (a keyed instance is an allocation each); give the session a stable id with an idempotency key and use suspend.durability = "store" to keep it across nodes
```

**Alternatives.** Allow promotion as for other sessions, making the isolate
a keyed instance with an allocation and a raft row.

**Trade-off.** No `instance list`, raft-backed rename, or keyed guarantees
for isolates; a platform that wants those for an agent runs it on a
`sandbox` or `exec` task.

**Why (performance).** A keyed instance is an allocation each, placed by
the scheduler lane, which would turn an isolate's microsecond start into a
raft write and undo the density the tier exists for. Session durability
gives the property callers usually want from promotion (a stable id that
survives the node) without either.

**As built (2026-11-11).** Promotion is refused as designed, and also at
submit: a `stateful.promote` block on an isolate task is refused, and
`CreateInstance` with an `instance_id` on an isolate task is
`INVALID_ARGUMENT` before any write (entry 42). The second half of the
call is not available: `suspend.durability = "store"` is not built for
isolates (entry 44), so today a session isolate does not survive its node,
and the refusal no longer recommends it:

```text
FAILED_PRECONDITION: isolate sessions cannot be promoted (a keyed instance is an allocation each); give the session a stable id with an idempotency key, and keep what must outlive its node in an upstream store
```

(Its `legion:isolate/state` half overstates: with `local` checkpoints the
state map is lost with the node too; only an upstream store outlives it.)

---

## 25. Preemption by epoch, yielding at the end of a slice; fuel only on request

**Call.** A ticker advances the engine epoch every millisecond; a turn
that exceeds its 5 ms slice yields to its shard instead of trapping, and
the invocation's CPU budget is checked at each yield.

**Alternatives.** Fuel metering, as the network plugins use: exact and
deterministic.

**Trade-off.** About 10% on compute-heavy code (an MMU-based variant under
review cuts SpiderMonkey's overhead to 2.8%), and CPU limits are
millisecond-granular, not instruction-exact.

**Why (performance).** Epochs are 2 to 3× cheaper than fuel, and
time-slicing a CPU-bound guest instead of killing it is what keeps one
tenant from starving a shard.

**As built (2026-11-11).** The epoch deadline is one tick, and the
callback checks the wall and CPU budgets on every tick, yielding only once
the turn has used `isolate.slice` of CPU; a CPU stop overshoots by at most
about one tick plus one bulk operation. The CPU charged to a turn is the
shard thread's CPU for the whole poll, host functions included. A wall
limit reached while the guest awaits cancels the call and discards the
instance. Measured overhead was higher than "about 10%": +14 to +25% on
call-heavy code, +1 to 3% on a tight loop, against +46% for fuel. Fuel is
not offered at all.

---

## 26. Each host runs one shard per core, shared nothing

**Call.** A host's isolates are partitioned across pinned shard threads,
each a single-threaded executor; an isolate runs only on its shard and
moves only between turns.

**Alternatives.** A work-stealing runtime (tokio multi-thread).

**Trade-off.** Imbalance needs an explicit rebalancer.

**Why (performance).** No lock in a turn, cache-warm isolates, and slice
accounting that never follows a task across cores. Redpanda reported 3×
throughput running wasmtime on Seastar's reactor threads.

**As built (2026-11-11).** Shards are pinned single-threaded executors
with deficit round robin per tenant, beside a 2-thread tokio runtime for
the host's two sockets and timers. The rebalancer is not built: a tenant
or session holder stays on the shard it was created on, request isolates
go to the less loaded of two random shards, and the shard count is fixed
when the host starts (`isolate.shards`, default one per CPU the host may
run on).

---

## 27. Runs gained `tenant`, `session_id`, and `args`

**Call.** `RunSessionRequest` gains three isolate-only fields beside
`tier`: `tenant` (the tenant key for a `tenant`-lifetime task; 1 to 128
bytes of printable ASCII without spaces, the same rule as the
`Legion-Tenant` ingress header), `session_id` (run inside a session
isolate, routed by its node tag), and `args`. A command component sees
WASI `args` of `[task name] + args`; a runner gets them in its request;
a VM-tier run appends them to the VM task's own `run.command`, so
`allowCommand` is not involved. All four are part of the idempotency
fingerprint for every run.

```console
$ legion session run code-runner --task run --file main.py --tenant acme --arg --verbose
$ legion session run code-runner --task run --file step.py --session run-3f.w17
```

```text
INVALID_ARGUMENT tier, tenant applies only to tasks of the isolate runtime; task machine of job code-runner runs on noop
INVALID_ARGUMENT 65 args, over the 64-argument maximum for one run
INVALID_ARGUMENT task run of job code-runner runs on the isolate runtime, which has no command to replace: a run executes the files you send (main.<ext>, else the first)
```

**Alternatives.** (a) The plan's surface, which had none of the three: a
tenant key only from an HTTP header, arguments only through
`input.json`. (b) Reuse `command` for arguments.

**Trade-off.** Three more fields that every other runtime refuses. Args
are capped at 64 and 64 KiB together. Adding the fields to the fingerprint
changed the scoped key of existing non-isolate runs (no installed base).

**Why (ease).** A tenant key is how a platform says whose warm instance to
use, and the plan gave a `RunSession` caller no way to say it; a session
isolate has to be addressable by `RunSession` as well as by HTTP; and
`argv` is how a component or a script expects options. `command` is
refused because an isolate task has nothing to replace: it runs the files
it is sent.

---

## 28. Reuse needs the `legion:isolate/runner` export; `wasi:cli/run` is single-shot, with exit 0 or 1

**Call.** A component that exports only `wasi:cli/run` gets a fresh
instance per run and serves `lifetime = "request"` only. Its exit status
is whatever WASI 0.2's `exit` carries, ok or err: Rust's std on wasip2
calls `exit(err)` for any nonzero code, so `exit(3)` reports 1. A
component (or image) that exports `legion:isolate/runner` can be reused,
reports exact exit codes through its `outcome`; `tenant` and `session`
lifetimes need it (or, for HTTP only, `wasi:http/incoming-handler`), and
the host refuses the task otherwise:

```text
lifetime "LIFETIME_TENANT" needs a component that exports legion:isolate/runner or wasi:http/incoming-handler; wasi:cli/run is single-shot
```

On a reused instance the run directory is
cleared before every run and `state` is untouched; the Python image keeps
one `__main__` namespace per instance (its `result` cleared per run),
while the JS image evaluates the entry as an async function body, so only
`globalThis` persists across runs.

**Alternatives.** (a) Link the unstable `exit-with-code` interface.
(b) Call `wasi:cli/run` repeatedly on one instance. (c) Parse stderr for
a code.

**Trade-off.** Commands report only 0 or 1, which the public doc has to
say; a guest that wants reuse or exact codes implements a Legion-specific
export.

**Why (simplicity, honesty).** Calling `run` twice on one instance is not a
contract any toolchain honors, and (a) and (c) would make exit codes
depend on the guest's toolchain version. A small export of our own is a
contract we control.

---

## 29. `entry` arrives through the task's artifacts and is staged per call, not pre-initialized

**Call.** `config.entry` is a relative path under the task directory or
`${artifact.<name>}`, and needs at least one artifact; artifacts on an
isolate task without `entry` are refused. The supervisor stages the
task's artifacts itself (the task has no allocation). The entry is copied
into the run directory and executed by the image on every fresh instance;
nothing is pre-initialized at stage time. A run's own source is
`main.<ext>` if the request has one (`js`: `.mjs` then `.js`; `python`:
`.py`), else the first file.

```nickel
config = { image = "js", entry = "${artifact.handler}" },
artifacts = [{ name = "handler", source = "https://example.com/handler.js", sha256 = "..." }],
```

**Alternatives.** (a) The plan's stage-time pre-initialization: run the
image with the script under Wizer and cache the snapshot as derived
content (`preinit-entry`, §6.5). (b) `entry = { source, sha256 }`, fetched
like `module`.

**Trade-off.** An entry pays its parse and top-level run on every fresh
instance, so the plan's 300 µs pre-initialized JS target is not met (a
fresh `js` hello world is 1.14 ms). The `preinit-image` and
`preinit-entry` derived kinds exist with no builder.

**Why.** wasmtime-wizer 50 can pre-initialize a component, but re-snapshotting
a componentize-js or componentize-py image needs that toolchain, which
the node does not have; doing it right is its own piece of work (open
TODO item). Delivering the file through `artifacts` reuses the pinning,
auth, and checksums every other runtime has.

---

## 30. Session isolates are reached through their id's node tag; untagged ids need `--on-node`

**Call.** `sessions::find` and `sessions::locate` already ask the node a
minted, tagged id names (`Worker.GetSession`) before they read the index,
and the worker answers for a session isolate with a synthesized
`SessionStatus` (`running` or `suspended`). So `GetInstance`, stop,
destroy, suspend, resume, resize, and promote reach a session isolate with
no new code path, with the coarse `AclScope::any()` check first and the
job scope enforced on the job the node reports. `RunSession` with a
`session_id` goes only to the node its tag names. `legion isolate get
<SESSION_ID> [--on-node N]` (`Manager.GetIsolate`) reports the session's
tier (resident tier, `hibernated`, or `empty`) and its child VMs.

```text
NOT_FOUND session isolate run-key-7 carries no node tag (an id derived from an idempotency key), so the router cannot find its node
Hint: name its node with node_id (--on-node)
NOT_FOUND no node matches the tag of session isolate run-3f.w17 (the node left the cluster, or the id is mistyped)
```

**Alternatives.** (a) An "index miss, ask the node" fallback in each
handler. (b) Scan the job's tasks for isolates before asking.

**Trade-off.** An id derived from an idempotency key carries no tag, and
none of the instance verbs can find it: only `GetIsolate` and
`RunSession` accept `node_id`.

**Why (simplicity, performance).** One forwarded call, only for ids with
the minted shape, already existed and was already ACL-ordered correctly.

---

## 31. Disabling the isolate runtime: the node decides when a VM can take the run, and a drain hibernates only what it covers

**Call.** The manager refuses a run up front (`runtime_disabled`,
UNAVAILABLE, retryable) only when it cannot go to a VM: `tier = "isolate"`,
or a task with no VM task. A run with `tier` unset, `auto`, or `vm` on a
task with a VM task goes to the node, which runs it in the VM with
`tierReason` saying the isolate runtime is disabled. Session-isolate
starts are refused up front. `--drain` and `SuspendSessions` hibernate
narrowly: the host's `Reclaim{HIBERNATE}` carries the covered `job_id`, or
the named `session_ids` (a named session with a call in flight is skipped
and the suspend answers "busy"); the pressure response's `suspend` step
stays host-wide. Building this fixed a bug outside isolates: the eval loop's and
`RuntimeRunning`'s session checks compared `stateful.mode == "session"`,
which a stored (normalized) session task never has, so a disabled
runtime's session tasks, sandbox sessions included, were never matched;
both use `session::is_session_task` now.

**Alternatives.** (a) Always refuse up front, the runtime-disable plan's
rule for every runtime. (b) Never refuse up front. (c) Host-wide
hibernation for drain and named suspends (the first build).

**Trade-off.** When every node refuses (say the VM task's runtime is
disabled too), the caller sees `RESOURCE_EXHAUSTED` "no node accepted the
run (…runtime_disabled…)" rather than one clean UNAVAILABLE.
`operator runtime status` counts 0 sessions for `isolate`, because
isolates are never in the index (`legion isolate stats` has the numbers),
and the submit warning `[runtime-disabled]` still fires for an isolate task
whose `auto` runs go to its VM.

**Why (ease).** The CVE kill switch must not break the VM tier, which is
the plan's own CVE response (§13). Refusing what cannot run anywhere keeps
the precise refusal where it is cheap. A narrowed hibernate keeps a drain
of one job from waking every other tenant of the cordon cold.

---

## 32. The default `js` image is StarlingMonkey; QuickJS is a candidate `js-small`

**Call.** `image = "js"` is SpiderMonkey compiled to Wasm with weval AOT.
QuickJS ships as `js-small` only if Phase 0 shows a several-times smaller
idle footprint.

**Alternatives.** QuickJS as the default (Shopify's choice): about 1 MB,
reference counting, smaller private heaps.

**Trade-off.** A larger image (~8 MB, shared copy-on-write) and possibly
larger private heaps per isolate.

**Why (ease).** Modern JS and Web APIs (`fetch`, streams, crypto) work
unchanged, which is what model-written code assumes; it is what Fastly and
Spin run in production; and weval closes much of the interpreter gap.

**As built (2026-11-11).** StarlingMonkey via componentize-js 0.23.0, but
*without* weval AOT: on dynamic source it gave no consistent gain (hello
world fresh 0.94 against 1.14 ms, a 12 KB snippet 2.70 against 2.86 ms,
warm 1.96 against 1.86 ms) for more idle memory (883 against 767 KiB) and a
19.1 MB image instead of 13.1 MB; weval specializes code known at build
time, which a snippet is not. `LEGION_JS_AOT=1` builds the variant to
measure again. Phase 0 met the `js-small` condition (QuickJS idle 127.6
KiB, 5.1× smaller than StarlingMonkey's 651.2 KiB in the spike), but
`js-small` was not built: it is a second JS engine's compatibility surface
with no Web APIs by default, and is an open item.

---

## 33. Python is CPython for WASI; heavy packages run in a VM

**Call.** `image = "python"` is CPython built for WASI (componentize-py),
pre-initialized with the standard library. Native-extension packages are
available only in curated images with working Wasm builds; anything else
is routed to a VM by the pre-flight check.

**Alternatives.** Pyodide, Cloudflare's choice for Python Workers.

**Trade-off.** Fewer packages than Pyodide's ecosystem; data-science code
mostly runs in a VM.

**Why (fit).** Pyodide targets Emscripten and a JS host, not WASI, and
starts in about a second with real packages even with Cloudflare's
snapshot. CPython for WASI pre-initializes into a component wasmtime
instantiates like any other.

**As built (2026-11-11).** CPython 3.14 via componentize-py 0.25.1, with
the whole standard library (entry 34) and no packages beyond it: no
curated images were built. A fresh hello world is 2.55 ms and a 12 KB
snippet 12.0 ms, so the plan's ≤ 5 ms holds for small snippets only.

---

## 34. The Python standard library ships as a compiled, compressed blob, loaded on first import

**Call.** componentize-py snapshots the interpreter after the runner
module ran but ships none of the library's files, so at run time any
module not imported during the build failed with `ModuleNotFoundError`
(found by test: `import secrets` and `import asyncio` failed in the first
image). The image build now compiles every standard-library module
(`marshal` plus zlib, one ~4.3 MB bytes object, 520 modules) and installs a
`sys.meta_path` finder that loads a module from it on first import. Twenty
common modules are also imported into the snapshot.

**Alternatives.** (a) Import every module at build time: works, but a 38
MB image and 8.1 to 8.3 MiB private per idle instance, because a run's
allocations land in the fragmented heap and refcount writes touch many
snapshot pages. (b) A fixed common list only, the rest silently missing.
(c) A read-only stdlib mount in every host.

**Trade-off.** The first import of a non-preloaded module pays
decompression and unmarshalling, and standard-library frames in tracebacks
show no source line. The image is 29.5 MB instead of 22. The preload buys
a fresh 20-module run in 2.2 ms instead of 10.7 ms and 1.5 MiB idle instead
of 3.0 MiB, for about 0.2 MB of image.

**Why (simplicity, density).** The whole library works as users expect,
idle private memory is about 1.5 MiB instead of 8, and nothing new enters
the host.

---

## 35. A JS entry is a script; ES modules are `unsupported`; files go through `legion.fs`

**Call.** StarlingMonkey 0.23 cannot link a module from source at run time
(`import('legion/run/x.mjs')` fails with `module record has unexpected
status: New`). So the entry is evaluated as the body of an async function
(top-level `await` and `return` work; the result is the return value, else
a top-level `result` binding, else `globalThis.result`), and an entry with
`import`/`export` that does not compile as a script stops the run. The
image's module list is empty, so the pre-flight check never sends JS to a
VM for an import. StarlingMonkey has no file API, so the run directory is
exposed as `legion.fs` (`readFile`, `readText`, `writeFile`, `list`,
`mkdir`, `remove`, `exists`); `require` of a Node builtin stops the run.

```text
ES module syntax (import/export): modules are not available in an isolate; write the entry as a script or run with --tier vm
require('fs'): Node builtins are not available in an isolate
```

**Alternatives.** (a) A source transform of `import`/`export` into a
loader (regex rewriting of user code). (b) Indirect `eval` as a global
script (a second run in a tenant isolate throws "redeclaration of let").
(c) A partial Node `fs` shim.

**Trade-off.** Code that uses modules always stops or goes to a VM;
top-level `let`/`const` do not persist across runs of a reused instance,
unlike Python's `__main__`; no npm packages.

**Why (honesty).** The run names what is missing and the memo sends the
next run of that code to a VM, instead of an engine error nobody can act
on, and a partial `fs` would fail in ways users would not expect.

---

## 36. Child VMs of an isolate are owned per isolate, and judged by its node

**Call.** A session isolate owns its children as `{kind: "session", id:
<session id>}`: they outlive its invocations and its hibernation, and it
re-attaches by id. A request or tenant isolate owns them under a new owner
kind, `{kind: "isolate", id: <invocation id>}`, and they end with the
invocation. The worker keeps a registry of isolate owners on the task
manager (`isolate_owners`), so the unchanged `check_child` enforces the
*isolate* task's `children.tasks` and `children.max`, and the children
reconcile ends a child whose owner is no longer registered; session
isolates restored after a worker restart are registered again before the
first reconcile. Managers skip `isolate` owners in their owner notices
(no manager knows of a request isolate) and judge session-isolate owners
through the tagged-node probe (entry 30), with the existing grace and
`orphanAfter` rules.

**Alternatives.** (a) A `session` owner with the invocation id (no such
session exists). (b) The envelope allocation as owner (every isolate of a
task would share one owner, so `children.max` would be per task and one
isolate's end would end all children). (c) A liveness trait the supervisor
answers. (d) Isolates in the claims table.

**Trade-off.** Liveness is "registered", not "asked": an isolate the
supervisor loses without ending its owner keeps its children until the
registration is dropped. A child's `SessionStatus.owner` shows a kind that
names no addressable object for request isolates.

**Why (simplicity, honesty).** Per-isolate `children.max` and "children
end with their owner" need a per-isolate owner, and a distinct kind is
honest in listings. The registry is the smallest hook that needs nothing
from the supervisor beyond telling it an owner ended.

---

## 37. Egress: the worker dials the envelope's own datapath listeners, and upstreams are addressed by name

**Call.** An isolate's `wasi:http` request is carried to the worker, which
connects to the envelope's listener sockets as a client: absolute-URI
requests for `http://`, `CONNECT` then worker-originated TLS (webpki
roots) for `https://`. A URL host equal to one of the envelope's upstream
keys, or `<key>.svc.legion`, goes to that upstream's listener (any port in
the URL ignored, `https://<key>` included: the upstream's own `to` decides
the last hop's encryption); any other host goes to the egress proxy. A
datapath refusal (`403` with `Legion-Denied-By`) reaches the guest as a
typed denial. Hop-by-hop headers, `Host`, `Content-Length`,
`Legion-Tenant`, and `Legion-Session` are never forwarded.

```python
legion.fetch("http://openai/v1/models")          # the connect upstream "openai", credential injected
legion.fetch("https://api.example.com/x")        # the egress proxy, under the task's policy
```

**Alternatives.** (a) Call the proxy's code in process
(`forward_absolute`, `egress::judge`). (b) Only the bare key as an
upstream address. (c) The containers' `LEGION_UPSTREAM_<KEY>` VIP form.

**Trade-off.** A socket hop per request and no connection pooling (not
built), HTTP/1.1 only to origins. A public host whose name equals an
upstream key cannot be reached through the proxy. An origin that itself
answers `403` with `Legion-Denied-By` over plaintext is reported as a
denial.

**Why (simplicity, security).** Every check (gate, policy, handlers, host
guard, credential injection, denial and activity ledgers, access log)
applies with no new code path in netd. Names match the containers'
name-is-config (`<key>.svc.legion`); there are no VIPs in an isolate's
world.

---

## 38. Ingress: one Unix socket per port label, HTTP/1.1 inside, bodies buffered

**Call.** For each of the isolate task's `network.ports`, the node's
ingress listener for the envelope allocation dials `ingress.sock_<label>`,
which the supervisor serves as HTTP/1.1 and hands to the isolate's
`wasi:http/incoming-handler`. `Legion-Tenant` picks the tenant instance
(absent: the anonymous tenant; invalid: `400` naming the header) and
`Legion-Session` the session isolate; both are stripped before the guest
sees the request. Bodies are buffered in both directions, up to
`isolate.maxBodyBytes` (default 16 MiB). A stopped invocation answers `503`
for `limits` and `500` otherwise, with a plain-text body; a guest status
outside 100 to 999 is `502`. The guest sees scheme `http` even when the
port terminates TLS.

**Alternatives.** HTTP/2 on the inner hop; streaming bodies; the plan's
"no TCP hop" dispatch straight into the host.

**Trade-off.** No streaming, no WebSockets, and a request body larger
than the cap is refused; the inner hop is one more local connection.

**Why (simplicity).** The node's ingress listener already terminates TLS
and applies the chain, so the inner hop is node-local plumbing, and
buffering is what the host protocol's unary `Invoke` carries.

---

## 39. A cordon's capacity: 10,000 slots per host, clamped by `vm.max_map_count`; replicas only grow

**Call.** `isolate.slotsPerHost` defaults to 10,000 and is capped per
layout at 16,000 (`guard-pages`), 200,000 (`mpk`), and 1,000,000
(`small`). The worker clamps a host's slots to what the node's
`vm.max_map_count` allows (about 8 mappings per live instance; the default
65,530 allows about 8,000) and says so. A cordon runs
`ceil(sum of its tasks' max_resident / slots)` host replicas per node,
recomputed when a task is prepared or reconfigured, with each task's
budget split evenly. Request calls go round robin over replicas, a tenant
to `hash(tenant) % replicas`, and a new session to the replica holding the
fewest sessions. There is no worker setting for the host's own memory: its
cgroup ceiling is the sum of its envelopes (entry 21).

**Alternatives.** (a) wasmtime's default pool of 1,000. (b) 16,000. (c)
The plan's §8: move room between replicas with the elastic envelopes'
grow-and-shrink rule.

**Trade-off.** 10,000 guard-page slots reserve 60 to 80 TiB of the 128
TiB address space. Uneven load can refuse on one replica while another
has room, and surplus replicas run until the cordon empties: the
rebalancer and shrinking are not built.

**Why (performance, simplicity).** 1,000 would start a second process per
cordon at a density the plan targets per process; 16,000 leaves no room
for the host's own mappings, stacks, and code. Phase 0 found the map count
binds before address space for MPK and dense hosts, so the worker must
respect it. Grow-only is simple and testable with tiny slot counts.

---

## 40. Admission is by reservation and CPU weight, enforced by the host per invocation

**Call.** The worker tells each host a task's `max_resident` (the
envelope's memory over the task's reservation) and `max_active` (the
envelope's CPU over the task's `resources.cpu` weight, at least 1),
split across replicas. The host enforces them per invocation with no
worker round trip: each live instance is charged its own reservation (a
resized session's, else the task's); an instance that does not fit evicts
the task's coldest idle tenant instance, else waits FIFO until
`edgeBoot.queueTimeout` and is refused (`RESOURCE_EXHAUSTED`, retryable).
A session resize checks only that the new reservation fits the whole
envelope; `memoryMax` above `isolate.maxMemory` answers `needs_restart`,
and `cpu` answers `not_supported`:

```text
not_supported: an isolate's cpu is its task's weight in the host's scheduler, not a per-session amount; change resources.cpu on the task
```

**Alternatives.** Count instances only; count cores; refuse without
evicting.

**Trade-off.** Eviction makes a tenant's next call start fresh. Resize
admission is coarse, with the host's accounting as the real check.

**Why (performance, simplicity).** Resized sessions must charge their own
reservation; idle tenant instances are a cache the plan says goes under
pressure; and charging each active invocation its declared weight matches
how `resources.cpu` is described.

---

## 41. The router reserves nothing for isolate runs and session-isolate starts

**Call.** `route_needs(task)` is empty for an isolate task: the router
still picks among nodes holding the task's envelope (by
`stateful.placement`, which with no needs is a random spread), counts
nothing in flight, and records nothing in the session index. The node
admits each isolate against its envelope; a node's `RESOURCE_EXHAUSTED`
moves the run to the next node, as for any session run.

**Alternatives.** Reserve one isolate's `resources` per run, as for other
sessions.

**Trade-off.** No in-flight balancing between nodes for a burst; the
spread is random.

**Why (performance).** Plan §8 item 3: an isolate start takes no launch
slot and is admitted by its host. The managers' room view says nothing
about isolates (they are never in a session report), so a reservation
could only throttle a burst the node would queue: after one refusal, the
second attempt would find "every envelope full" without asking. Tested:
64 concurrent runs all reach the node.

---

## 42. A session-isolate start is confirmed by the node's reply, and is never a keyed instance

**Call.** `sessions::start` on an isolate task records nothing in the
index (a record would show a phantom session in `instance list` for 30 s
and count room) and answers `running`. A start on a task whose lifetime is
not `session` is `FAILED_PRECONDITION`; `CreateInstance` with an
`instance_id` on an isolate task is `INVALID_ARGUMENT` before any write. A
keyed (idempotency-derived) start remembers the node it was sent to, per
manager, for 10 minutes, so a retry through the same manager finds the
running isolate.

```text
task run of job code-runner is an isolate task with config.lifetime = "request": its isolates live for one run and cannot be started as sessions
Hint: run code in it with `legion session run code-runner --task run` (RunSession), or set config.lifetime = "session" for addressable session isolates
task run runs on the isolate runtime, whose isolates are never keyed instances; omit instance_id
Hint: pass an idempotency key for a stable session id
```

**Alternatives.** Record isolate starts in the index like other sessions;
let a named create make a keyed instance.

**Trade-off.** A retry through *another* manager can start a duplicate on
another node; the index's duplicate sweep does not see isolates.

**Why (simplicity).** Entry 4 keeps isolates out of the index; a short
per-manager memo covers the common retry without a replicated record.

---

## 43. `isolateCordons`: exact kinds, every governing policy must allow the job's; `shared` must be listed

**Call.** Namespace policy `isolateCordons` takes `namespace`, `job`, and
`shared` (no globs), stored in that order. Every governing policy that
sets the list must include a job's kind; `shared` additionally needs at
least one governing policy that lists it, and is refused when no policy
governs the namespace at all. The check runs before admission's early
return for "no governing policy". The manager's policy dry run checks
narrowed one-rule copies of a policy, which now carry all three kinds plus
an `only("isolateCordons", …)` rule when the policy sets the list, so an
existing shared-cordon job is reported only under `isolateCordons`.

```text
[isolate] tenants/code-runner/run: config.cordon = "shared" is refused: no namespace policy governing namespace tenants allows shared cordons, which run several namespaces' isolates in one process
Hint: use cordon = "namespace" (the default), or have an operator list "shared" in a namespace policy's isolateCordons (isolateCordons = ["namespace", "job", "shared"])
```

**Alternatives.** Globs like `runtimes`; allow `shared` unless a policy
forbids it.

**Trade-off.** An operator has to write a policy before any namespace can
use `shared`, even on a single-tenant cluster.

**Why (security).** `shared` mixes namespaces in one process, which is the
one cordon decision that crosses Legion's trust boundary; it should never
happen by default.

---

## 44. `suspend` on a session isolate: what applies, what is refused, and `durability = "store"` is not built

**Call.** `suspend` is allowed only with `config.lifetime = "session"`.
`idle`, `keepFor`, `onWorkerStop`, `onPressure`, `onRestoreFailure`, and
`resumeTimeout` apply; `checkpoint` (either kind), `checkpointEvery`,
`archive`, `park`, `onResume`, and `wake` are refused with a hint (a guest
parks with `legion.park()`, not the block). `durability = "store"` is
refused at submit; the node refuses it too, as a backstop for a job stored
before the check, and answers that task's runs and starts with
`FAILED_PRECONDITION`. Checkpoints stay on the node that took them.

```text
[isolate] job/run: suspend.checkpointEvery does not apply to an isolate: an isolate hibernates to its own checkpoint format at a turn boundary, and wakes on its next request
Hint: remove suspend.checkpointEvery; idle.after, keepFor, durability, and onWorkerStop apply
[isolate] job/run: suspend.durability = "store" is not available for isolates yet: a session isolate's checkpoint stays on the node that took it, so it wakes only there and is lost with the node
Hint: remove suspend.durability (the default is "local"), and keep what must outlive the node in an upstream store
```

**Alternatives.** Reuse `checkpointEvery` and `archive` as hibernation
knobs; publish isolate checkpoints with a session manifest now.

**Trade-off.** No periodic or archived isolate checkpoints, and no session
isolate survives its node; the plan's durable named isolate (entry 24)
waits on this.

**Why.** Those fields are defined in terms of VM memory images and
volumes; reusing their names for a different format would mislead.
Publishing an isolate checkpoint as a session manifest needs a second
publication path, deferred rather than half-built.

---

## 45. Default limits: `memoryMax` 128 MiB (4 GiB at most), 1 MiB output; CPU and wall required

**Call.** `resources.memoryMax` defaults to 128 MiB with a worker-wide
maximum of 4 GiB, and `limits.output` to 1 MiB. `limits.cpu` and
`limits.wall` have no defaults and are required at submit.

**Alternatives.** Defaults for CPU and wall too (Workers: 30 s CPU on paid
plans).

**Trade-off.** One more thing to write in every isolate task.

**Why (ease of debugging).** 128 MiB is Workers' limit and sits under the
~550 MB at which MPK striping gets its full 15×. CPU and wall budgets vary
by three orders of magnitude between a webhook and an agent loop; a wrong
default would either kill legitimate work or let runaway snippets burn a
shard, and a required field puts the decision where the author sees it.

**As built (2026-11-11).** As designed, with the session-run bounds: `wall`
at most 24 h, `output` at most 1 MiB, and `cpu` above `wall` refused,
because an isolate runs on one thread. The worker-wide maximum is
`isolate.maxMemory` (1 MiB to 4 GiB).

---

## 46. `limits.output` truncates; `cpu`, `wall`, and memory stop the run

**Call.** Output past `limits.output` is cut and the reply says
`truncated`, as `run.output` does for session runs; it does not stop the
run. stdout and stderr share the budget in arrival order, then files
returned with `get` take what is left, in request order.

**Alternatives.** The plan's list, where every limit stops the run with
`stopped.reason = "limits"`.

**Trade-off.** A run that prints forever runs until its CPU or wall limit;
`limit = "output"` exists in the proto for a stop that never happens.

**Why (consistency).** Session runs already truncate (session-runs calls
entry 10), and an isolate run is a session run; two rules for one reply
field would surprise.

---

## 47. wasi-libc guests start in `/`; the run directory is `/legion/run`

**Call.** The scratch tree's preopens are `.` (the run directory) then
`/`, and the host sets `initial_cwd("/legion/run")`. wasi-libc never reads
`initial-cwd` and strips both preopen names to the same empty prefix,
where the last listed wins, so with `/` last absolute paths work and
relative paths resolve from `/`, which is also what `getcwd()` reports.
The images change into `/legion/run` before running the entry; a
component reads `LEGION_RUN_DIR` or uses absolute paths.

```text
open("input.json")              -> /input.json in a wasi-libc command (not found)
open("/legion/run/input.json")  -> the run's input
```

**Alternatives.** (a) `.` last: relative paths land in the run directory
but every absolute path breaks. (b) Make the root *be* the run directory
and alias `/legion/run` to it: a directory cycle recursive walkers follow
forever. (c) Name the cwd preopen `/legion/run`: relative paths still
resolve from `/`.

**Trade-off.** A command that opens `input.json` by a relative path
without changing directory reads the wrong file; the public doc says so.
A test asserts `initial_cwd=/` so a toolchain that starts honoring it is
noticed.

**Why (correctness).** The only order where every absolute path works and
the cwd a guest reports matches where its relative paths go.

---

## 48. Every clock a guest sees is the turn's coarse, frozen clock, and every instance gets its own seed

**Call.** Both WASI clocks and the scratch tree's timestamps read one
1 ms-resolution reading taken at the start of the turn; the tree never
reads a clock of its own. `wasi:random` is a generator seeded from the
kernel per instance. The images reseed what a snapshot froze on each
instance's first run: Python's `random`, `uuid`'s clock state,
`tempfile`'s names, and `os.environ` (both images re-read the
environment); StarlingMonkey's `Math.random` and `crypto` were measured to
differ between instances already. Restore hygiene is tested where the
snapshots are made, in `crates/isolate` (`restore_hygiene_*` in
`tests/run.rs` and the image tests), not in the plan's shared
`tests/integration/restore_hygiene.rs`, which does not exist.

**Alternatives.** File timestamps from `SystemTime::now()` (the first
version); one shared conformance test for VM forks, prepared VMs, and
isolates.

**Trade-off.** A file written in a new turn before any filesystem call can
carry the previous turn's time. Python's string hash seed is fixed per
image build and shared by every instance of it. Restore hygiene has no
single cross-runtime test.

**Why (security).** A timestamp is a clock: stamping files from the real
clock gave guests a nanosecond timer (write, `stat`, repeat) around the
clock coarsening of plan §6.10 layer 4. The shared test would need a
second snapshotting runtime in the same harness; the isolate cases test
the same property beside the code that makes the instances.

---

## 49. The runtime turns on where the binary is found; images are found beside it

**Call.** The worker uses `isolate.binary`, else `legion-isolate` beside
its own executable, else `LEGION_ISOLATE_BINARY`; and images from
`isolate.images`, else `isolate-images` beside the binary, else
`LEGION_ISOLATE_IMAGES` (a directory of `<name>.wasm` plus
`manifest.json`). A worker whose binary answers `engine-info` advertises
the `isolates` capability, and every isolate envelope gets an implicit
`capabilities contains "isolates"` constraint, so it is never placed on a
node that cannot run it. x86-64 Linux only.

**Alternatives.** An explicit opt-in flag per worker; images embedded in
the binary.

**Trade-off.** A cluster with mixed workers places isolate envelopes only
where the binary is; an operator who forgets the images directory gets
image tasks refused at prepare ("this node has no isolate images (set
isolate.images, or install isolate-images beside legion-isolate)") while
module tasks run. The capability does not say whether images are present.

**Why (simplicity).** The sandbox runtime already works this way with
`legion-guest`: shipping the binary is the opt-in. Keeping the images as
files beside it lets them be rebuilt and pinned without rebuilding the
host.

---

## 50. The VM task does not share the isolate's `connect` block yet

**Call.** A sandbox session task still may not declare `connect`, and a
sandbox session whose network resolves gated is refused, so the demo VM
tasks (`machine` in `configs/jobs/isolate.ncl`, `snippets-vm` in
`maximal.ncl`) set `network = { mode = "none" }` and no `connect`.

**Alternatives.** Extend session datapath support to sandbox sessions as
part of this work.

**Trade-off.** The plan's §7.1 binding (`connect = net` on both tasks) does
not validate, and Phase 5's acceptance criterion "a VM given the same
`connect` binding reaches the same upstream" is not met: a VM run or a
child VM has no upstreams and no injected credentials.

**Why.** Giving sandbox sessions a gated datapath is a change to the
sandbox runtime and the datapath, outside this feature; it is an open item.

---

## 51. The Python image stops unsupported calls by name, and reroutes stdlib networking through `legion.fetch`

**Call.** CPython for WASI lacks `_ctypes`, threads, sockets, `ssl`, and
processes. The image replaces each with a stub that stops the run as
`unsupported` naming the call: `ctypes` (left out of the module list so
the pre-flight check routes it), `threading.Thread.start`,
`os.fork`/`exec*`/`spawn*`/`system` (served by an `os.__getattr__` only to
the caller's own code, so the standard library's feature tests are
unaffected), and `subprocess`. `http.client` and `urllib.request` send
through `legion.fetch` (with an `HTTPSConnection` added, so `urlopen("https://…")`
works), and asyncio runs on the image's own event loop, whose socket and
subprocess methods stop the run by name.

```text
subprocess.run: processes are not available in an isolate
ctypes.CDLL: native libraries are not available in an isolate
threading.Thread.start: threads are not available in an isolate
```

**Alternatives.** Plain `ModuleNotFoundError`/`AttributeError`; stubs set
on `os` outright (tried: `random` saw `os.fork`, called
`register_at_fork`, and the image failed to build); componentize-py's
`PollLoop` (no timers).

**Trade-off.** A frame-inspection rule in `os`; `hasattr(os, "fork")` is
`True` for user code. Responses are buffered whole; the low-level
`putrequest`/`endheaders` path stops as a socket.

**Why (honesty).** Plan §6.2 rule 2: what an image cannot do, it says by
name, so the caller and the memo can act on it, and the standard library's
HTTP clients work instead of stopping.

---

## 52. A child VM handle is bounded, and reaches only the child's own ports

**Call.** `machine.fetch` dials the child through its attachment (vsock
for a gated sandbox, its namespace's loopback, its TAP address) on any
port; a child that shares the node's network reaches only its declared
ports. `put`/`get` paths are relative to the child's run directory
(absolute only in a child with its own root; `..` refused), capped at
`isolate.maxBodyBytes`. `exec` defaults to 60 s, is capped at 10 minutes,
and its output at `maxBodyBytes`. `acquire` waits 60 s for the child to
run, then up to 20 s for exec readiness.

**Alternatives.** Any port on the node's loopback for host-network
children; resume a suspended child on the next call.

**Trade-off.** A host-network child's undeclared ports are unreachable. A
child its own idle policy suspended answers `child-gone` ("… is
suspended …"); resuming it from the isolate is not built. Over-cap files
are `failed` (the WIT has no too-large variant).

**Why (security).** Otherwise an isolate could reach any node-local
service, the manager on `127.0.0.1:4747` included, through an `exec`
child.

---

## 53. A host restart fails its in-flight calls and loses resident sessions that never hibernated

**Call.** When a host exits, its in-flight calls fail `UNAVAILABLE` ("the
isolate host for cordon X exited during the call; … retry"), and it is
restarted with backoff from 200 ms doubling to 30 s; every task of the
cordon is loaded and configured again, and `restarts` counts it. A session
isolate with a checkpoint comes back hibernated; a resident one that never
hibernated is dropped and logged, and its next call answers `NOT_FOUND`.
The same holds across a worker restart (the session book is
`<state dir>/isolates/sessions.json`). Invocation ids are used only for
attribution, not idempotency, inside the host.

**Alternatives.** Checkpoint every session on a timer; keep invocation-level
idempotency in the host (plan §12).

**Trade-off.** A host crash loses the state of every session isolate that
had not hibernated. Run-level idempotency still holds per node (the
worker's), but a retried call that was mid-flight in the host runs again.

**Why (simplicity).** A periodic checkpoint is the `checkpointEvery` design
entry 44 defers; until then the guarantee is the one a `local` session
already has.

---

## 54. Guest logs go into the envelope allocation's task log, one prefixed line each, once per invocation

**Call.** Output a handler or session isolate prints is forwarded by the
host as one batch per invocation, when the invocation ends, and the worker
appends it to the *envelope* allocation's `stdout.log` / `stderr.log`,
where `legion job logs` reads it. The task log format has no fields, so
every line carries a compact prefix naming the isolate and its tenant. A
request isolate's run output goes in its reply and is never forwarded. The
active file rotates as soon as a batch takes it past
`LEGION_LOG_MAX_FILE_BYTES` (not only on the worker's periodic sweep),
keeping `LEGION_LOG_MAX_FILES` generations, and one line is cut at 16 KiB.
A supervisor with no log root (only one built by hand in a test) writes
the lines to the worker's debug trace instead.

```text
[isolate run-3f2a.1] hello from a session isolate
[isolate 7c1e… tenant=acme] handled GET /
```

**Alternatives.** (a) The worker's debug trace only (the first build).
(b) A log file per isolate. (c) A structured `isolate` field, which the
plan's §6.9 named, in a log format that does not have fields.

**Trade-off.** All of a task's isolates share one stream per node, told
apart only by the prefix; a long-running invocation's output appears only
when it ends; and a burst can rotate the file faster than a reader
follows it.

**Why (simplicity).** The envelope is the task's only allocation on the
node, so its log is where `legion job logs` already looks; a file per
isolate would be 10^5 files a node cannot bound. Rotating after every batch
keeps 10^5 isolates from outrunning the sweep.

---

## 55. Isolate metrics have no per-job labels, and count CPU in microseconds

**Call.** The `legion_isolate_*` families (`crates/common/src/telemetry.rs`)
carry no `job` or `task` label; per-task numbers travel on the heartbeat
to `legion isolate stats`. The CPU counter is
`legion_isolate_cpu_microseconds_total`, not the plan's
`_cpu_seconds_total`, because the registry's counters are integers.
`legion_isolate_vm_runs_total{reason}` uses `requested|disabled|import|memo`,
and a `legion_isolate_slots` family reports slot occupancy.

**Alternatives.** The plan's §6.13 "per task" labels.

**Trade-off.** A Prometheus user cannot break isolate metrics down by job.

**Why (performance).** No other worker family has job labels, and
per-task series would hit the registry's 256-series cap on a busy node.

---

## 56. `GetIsolateStats` asks the nodes this manager has no heartbeat from

**Call.** The manager keeps the latest `isolate_tasks`/`isolate_hosts`
per node in memory (replaced per beat, forgotten on an empty beat, swept
after 120 s like usage). A `GetIsolateStats` reply is built from that
table, then every node with an envelope for the task(s) missing from it is
asked directly (`Worker.GetIsolateStats`, concurrently, 2 s timeout); those
rows carry `ageMs = 0`, and a node that does not answer is left out.

**Alternatives.** Answer from this manager's table only, as
`GetNodeUsage` does; a peer fan-out between managers.

**Trade-off.** One worker call per such node per request; a slow node
costs up to 2 s.

**Why (ease).** The proto promises the latest from every node with an
envelope regardless of which manager answers, and nothing goes to raft.

---

## 57. Run replies add isolate keys only on isolate replies; the default task is the isolate task

**Call.** `tier`, `tierReason`, and `nextTier` appear in `legion session
run -j` only when the reply's `tier` is set; `vmMs` only when positive;
`isolateStart` when set; `ranTwice` only when true; `cpuMs` and `wallMs`
become fractional only for isolate replies; dry runs add `tier`,
`tierReason`, `imports`, `missingImports`. A non-isolate reply is
unchanged. Text mode prints the tier on stderr and leaves stdout to the
program. Exit status is 124 for a wall stop and 125 for any other stop. A
run with no `--task` on a job with an isolate task and its VM task picks
the isolate task. `legion isolate stats -j` is `{"task", "nodes"}` for one
task and `{"tasks": [...]}` for several.

```text
legion: ran in the vm tier (fallback: stopped in the isolate, unsupported (subprocess.run)); your code ran twice, the second time in a VM
```

**Alternatives.** Always emit every key (one shape for agents); treat both
session tasks as candidates and make the caller name one; always `tasks`.

**Trade-off.** Agents test for key presence; running the VM task directly
needs `--task machine`.

**Why (ease).** Existing session-run consumers see no change, and the VM
task exists to serve the isolate task (`tier = "vm"` reaches it).

---

## 58. Isolate config refuses unknown fields; a `file://` module is host-level

**Call.** Every isolate config struct, and the worker's `isolate` block,
denies unknown fields (the schema has `additionalProperties: false`).
`module.source` takes `https`, `http`, `s3`, `oci`, and `file`; a
`file://` module is reported as node-reaching, so it needs the `host`
grant and is refused in reserved namespaces, like a `file://` artifact.
`sha256` takes 64 hex digits or `sha256:<hex>`; an image is pinned as
`name@sha256:<hex>`.

```nickel
config = { image = "python", lifetme = "tenant" }   # refused at job validate
```

**Alternatives.** The sandbox runtime's catch-all for retired fields;
refuse `file://` outright.

**Trade-off.** A field added later is refused by an older validator.

**Why (ease, security).** The sandbox's catch-all exists for a runtime
with history and lets its typos pass; this one has none. Local
development wants `file://`, and the root worker fetching node files is
what the existing host-level rule is about.

---

## 59. The image manifest is written from the built images; builds are not reproducible, so a release pins the artifact

**Call.** `crates/isolate/images/build.sh` builds the images and then runs
`manifest_write_test` against them, which writes `manifest.json`: each
image's sha256, initial memory, the Python module list (each candidate
imported in a fresh instance of the built image), and idle private bytes
measured right then. Two builds from identical inputs give different bytes
(the snapshot captures random state and the hash seed), so the manifest
pins the sha256 of the artifact that was built; a release ships that
artifact, never a rebuild.

**Alternatives.** A checked-in measurements file copied by `build.sh`; a
module list from the build machine's own Python.

**Trade-off.** `build.sh` needs cargo and a test build (`--no-manifest`
skips it); idle bytes are a measurement of the build machine. Nobody can
verify a shipped image by rebuilding it.

**Why (correctness).** The module list the pre-flight check uses is what
the image really imports, and the numbers describe the image they ship
with. Reproducible snapshots are an open item.
