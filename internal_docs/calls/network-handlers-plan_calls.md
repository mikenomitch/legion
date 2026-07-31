# Judgement Calls: Network Handlers

Decisions taken while designing
[`plans/network-handlers-plan.md`](../plans/network-handlers-plan.md). The plan
is **PLANNED, not implemented** — these are design-time calls, and any that get
revisited during implementation should be edited in place with what actually
happened, not appended as a second opinion.

**Entries are ranked by project impact, highest first; an entry's number is its
rank.** Insert new entries at their rank and renumber rather than appending.

---

## 1. A handler is an out-of-process proxy, not an in-process plugin

**Call.** The extension tier for "run my own code on the network path" is
`handler: 'proxy'` — an ordinary Legion service or a sidecar task that Legion
connects to, with metadata in a PROXY protocol v2 header (TCP/UDP) or `Legion-*`
request headers (HTTP). There is no `plugins/nethandlers/` `cdylib` category,
now or planned. The only in-process tier is a closed `const HANDLERS` table of
built-ins that ship in the binary.

```jsonnet
egress: { chain: [{ handler: 'proxy', service: 'egress-guard', onError: 'deny' }] }
```

**Alternatives.** (a) A `cdylib` plugin kind, which is what every other
extensible surface in Legion is — task runtimes, scheduling policies, node
checks, rollout strategies, artifact fetchers — and which the recipe in
`docs/plugin-system.md` makes almost mechanical to add. (b) WASM handlers first,
as the mesh doc's Part D recommends for "arbitrary code between services".

**Trade-off.** A userspace hop per connection, and off-node latency when the
handler is a service rather than a sidecar. A `cdylib` handler would be a
function call. We also give up compile-time typing of the handler contract: a
handler that mis-frames its output produces a runtime failure, not a build error
— which is why `legion net handler test` is in the same phase as the feature.

**Why (simplicity, and the failure domain).** The mesh doc already wrote the
argument down and it applies with more force here: a panic in a `cdylib` handler
kills the worker's whole datapath, not one connection, and the datapath sees
*every* workload's traffic on the node — arbitrary native code there is a
cross-tenant exfiltration primitive. The `async` datapath makes it worse, since
the dylib ABI is hostile to `async` traits. Out-of-process costs a hop and buys
a crash boundary, a resource boundary, any language, and — the part that matters
most — **zero new machinery**: a handler is a task, so it already has images,
artifacts, resources, health checks, restart policy, logs, ACLs, and rollouts.
WASM (plan phase 8) is strictly better than `cdylib` and strictly more work than
a service; shipping the service tier first means the WASM tier can be judged on
demand rather than on speculation.

---

## 2. Sandbox egress is captured by removing the guest NIC, not by firewalling it

**Call.** `sandbox` gains `net: 'gated'`, in which the microVM has **no network
device at all**. A `legion-guest-agent` offers SOCKS5 + HTTP CONNECT on guest
loopback and tunnels every stream over `vsock` to the node-local chain; a
guest-side redirect catches processes that ignore `HTTP_PROXY`. Ingress arrives
the same way, in reverse.

**Alternatives.** (a) `mode: 'tap'` plus host `nftables` rules pointing at the
chain — the conventional answer, and the one the existing `TapNet` code is
already shaped for. (b) Build the planned datapath/CNI plugin first and do
capture properly with netns per allocation.

**Trade-off.** A guest agent is a new component that must be built into the root
disk, versioned with the host, and kept working across hypervisors. Every stream
pays a vsock hop. And it is `sandbox`-only: `container` on Linux runs in the
**host** network namespace today (asserted by test at `oci_bundle.rs:669`), so
container and `exec` egress stays cooperative proxy-env — which the plan's
capture table states to the user rather than hiding.

**Why (simplicity, and it is a security boundary).** A firewall rule set is a
configuration you can get wrong; an absent device is not. `gated` needs no
`CAP_NET_ADMIN`, no bridge, no host addressing plan, and no per-node
`nftables` state, which means it is the same on every worker and cannot drift.
The cost is also lower than it looks: the sandbox runtime already builds the
guest root disk and generates `/legion/init`, and two other planned features
want the same agent — VM forking needs it to reconfigure identity on resume
(`sandbox-snapshots-and-fast-start-plan.md`), and `exec` into a microVM is
blocked on exactly a vsock guest agent (`docs/task-runtime-sandbox.md`). One
component, three features. Option (b) is the right long-term datapath and
remains planned; it is not a prerequisite for this, and making it one would put
the core use case behind a subsystem nobody has started.

---

## 3. Egress ships before ingress, inverting the order the request stated

**Call.** Phases 0–2 build egress (chain core, HTTP mode, sandbox capture) and
the core use case is complete at phase 2. Ingress is phase 3.

**Alternatives.** Ingress first, which is the order the feature was asked for
("inject ingress and egress handlers"), and which is the easier demo — an
ingress proxy in front of a web service is a thing everyone can picture.

**Trade-off.** The mTLS scenario (S3) and the autoscaler's wake-on-request
dependency both wait three phases longer than they otherwise would.

**Why.** All three capabilities the request actually named — per-user
observability, dynamically locking down endpoints, filtering the context that
egresses — are **egress** capabilities. Ingress serves authentication, which
needs the workload CA anyway (call 6), so building ingress first would either
ship it without mTLS or drag the CA forward into phase 1. Egress also has a
seam already: one line at `forwarder.rs:444`. Ingress has none, and creating one
means changing what the catalog advertises versus what the task binds (call 4).
Cheaper first, and it is what was actually being asked for.

---

## 4. An ingress chain changes what `LEGION_PORT_<label>` contains, rather than adding a second variable

**Call.** With an ingress chain, Legion binds the *advertised* host port and the
task binds a private one — and `LEGION_PORT_<label>` holds the **private**
value. The catalog, `legion service resolve`, and health checks all target the
advertised port. `AllocPort` gains `private_port`.

**Alternatives.** Keep `LEGION_PORT_<label>` as the public port and add
`LEGION_BIND_<label>` for what the task should actually bind — which is more
explicit and does not change the meaning of an existing variable.

**Trade-off.** The value in `LEGION_PORT_http` is no longer necessarily the port
a consumer dials, so a task that *prints* its address for a human is now
printing the wrong one. And health checks traverse the chain, so a deny-all
ingress rule fails your own health check and rolls the deployment back.

**Why (simplicity).** The instruction every Legion job already follows is "bind
`$LEGION_PORT_<label>`", and it should stay true whether or not somebody put a
chain in front of you — a job author should be able to add an ingress chain to a
running service without touching the image. A second variable makes every job
author learn both and pick correctly, forever, to serve a case most of them will
never hit. The health-check consequence is arguably a feature — a check should
test what a consumer experiences — but it is a genuine footgun, so
`legion net doctor` names that case explicitly and the public doc states it.

---

## 5. Dynamic policy rides the heartbeat, fail-static; no KV watch is built

**Call.** `rulesFrom: 'kv://…'` is rendered by the manager and shipped in the
heartbeat reply next to `upstream_endpoints`, swapped only after a *successful*
heartbeat. A KV write reaches every live workload within one heartbeat (2s) with
no restart and no rollout; a manager outage keeps the last known good rules.

**Alternatives.** Add the `WatchKv` server-streaming RPC that `docs/kv-store.md`
readers keep expecting, and have each worker subscribe. It is the obvious
design, and it would be lower-latency and useful to more than this feature.

**Trade-off.** Up to one heartbeat interval of staleness, and net policy is
welded to the heartbeat's health — if heartbeats are failing you keep old
policy (which is the right failure, but it is a failure mode to explain). It
also declines to build something several other features would use.

**Why (simplicity, and the failure semantics).** There is no KV watch and no KV
broadcast channel in the store today; `WatchKv` means a second broadcast channel
on `Store`, a new streaming RPC, per-worker subscription lifecycle, and
reconnection semantics — a subsystem, to deliver 2 seconds. The heartbeat path
already exists, already carries a policy-shaped payload, and already has the
*correct* failure semantics for a security control: fail-static, last known good,
explicit-empty-is-authoritative. A watch stream would have to reinvent all three
and would get them wrong the first time. The 3am job-to-be-done is "under a
minute"; 2 seconds clears it by a factor of thirty.

---

## 6. mTLS ships bring-your-own-PKI first and a Legion workload CA second

**Call.** Phase 5a accepts operator-supplied cert/key/CA (files or
`${artifact.<name>}`) and can terminate, verify, and originate with them. Phase
5b adds `certFrom: 'legion'` — a cluster workload CA issuing short-lived certs
whose SAN is the workload-identity `sub`.

**Alternatives.** Build the CA first and only ever support Legion-issued certs.
It is one code path instead of two, and it is what makes intentions
cryptographic — the outstanding item the service-discovery plan has been waiting
on.

**Trade-off.** Two certificate-source code paths, and phase 5a ships an mTLS
story that does not yet make `service.allow` cryptographic — so for one phase
the docs have to say "mTLS, but intentions are still control-plane".

**Why.** There is genuinely no workload CA today — `src/certs.rs` is a dev-TLS
fixture generator sitting behind `#[allow(dead_code)]`, not an issuer — so 5b is
real new machinery: issuance, rotation, revocation, trust distribution, and a
new root of trust for the cluster. Every operator who already has an internal CA
can be served *without any of that*, and 5a is close to just wiring rustls to
paths. Shipping it first means the ingress chain is useful the moment it exists,
and the CA gets designed against a working consumer instead of in the abstract.

---

## 7. `protocol` is declared, never inferred from the shape of a rule

**Call.** `egress.protocol` defaults to `tcp`. A path in an `allow` entry or a
`path` match in a `tcp` chain is a **submit-time error** naming the field and
the fix, not a signal to switch the chain to HTTP.

```
egress.allow[1] 'api.github.com/repos/**' matches on a path, but
egress.protocol is 'tcp'. Set protocol: 'http' to match on paths, or
drop the path to match the host only.
```

**Alternatives.** Infer: an entry containing a `/` after the host means HTTP.
This is what most people would expect, it makes the one-liner cover both cases,
and it removes a line of boilerplate from the common sandbox spec.

**Trade-off.** The most useful one-liner — HTTP path rules for a sandbox — costs
an extra line. That is the exact line the progressive-disclosure grammar was
supposed to save.

**Why (simplicity, at 3am).** Inference here silently changes the *security
posture* of the chain: HTTP mode terminates and parses, matches on paths a TLS
chain cannot see, and behaves differently on a connection upgrade. A rule set
that quietly became L7 because someone typed a slash — or quietly stayed L4
because they typed a hostname with a trailing slash — is a policy you cannot
review by reading. This is the same class of bug the service-discovery plan
called out in Istio: config that instantly changes traffic behavior through a
default nobody looked at. One extra line, and the spec says what it does.

---

## 8. Per-user attribution goes to events and access logs, never to metric labels

**Call.** `network.labels` (e.g. `{ user: 'alice' }`) appears on every event and
every access-log line, and on **no** metric. Metrics keep bounded labels only:
`direction`, `protocol`, `verdict`, `handler`, and job/task where the job count
is bounded.

**Alternatives.** Put the label set on the metrics too, which is what "add o11y
per user" most directly asks for and what a Grafana dashboard wants.

**Trade-off.** "Requests per user" is a log query, not a PromQL query. Anyone
who wanted a per-user rate panel has to run it through their log pipeline.

**Why (it would not have worked).** The metrics registry caps a family at 256
label combinations and folds the overflow into `"other"`
(`docs/metrics.md`). A per-user label on a fleet of sandboxes blows that cap on
the first thousand users and **silently destroys the metric** — you would not
get expensive cardinality, you would get a broken series and a dashboard that
lies. Events and logs are the unbounded-cardinality surface Legion already has,
they are already streamed and already queryable, and a denial event carrying the
user and the matched rule is a better artifact for the billing and audit
questions than a counter is anyway.

---

## 9. Version skew is a placement constraint, not a best-effort ignore

**Call.** A manager must refuse to place an allocation with a chain on a worker
whose fingerprint does not advertise chain support. The allocation stays
unplaced with a legible reason rather than running.

**Alternatives.** Let it run — the older worker simply ignores the unknown
`task_net_chain` field, which is the ordinary proto forward-compatibility
behavior and what every other field in this system does. Managers upgrade first
per `docs/upgrades.md`, so the window is short.

**Trade-off.** A mixed-version cluster can leave chained jobs unplaceable
mid-upgrade, which looks like a capacity failure to anyone who has not read the
upgrade note. Chains become the first thing in Legion that gates placement on a
worker version.

**Why (security boundary).** The default forward-compat behavior here is
"quietly run the untrusted workload with its egress policy removed" — the single
worst outcome this feature can produce, and it would be invisible: the task is
Running, healthy, and unfiltered. An unplaced allocation with a clear reason is
a page; a silently unfiltered sandbox is an incident nobody opens. Anywhere a
field is a policy rather than a preference, ignoring it must be louder than
failing.

---

## 10. TLS `inspect` ships, but operator-gated and effectively sandbox-only

**Call.** `egress.tls.mode: 'inspect'` terminates TLS, runs the chain on
plaintext, and re-encrypts to the destination — a MITM. It requires the cluster
operator to set `network.allowTlsInspect: true`, and it is only workable where
Legion controls the trust store, which today means `sandbox`. The public doc
uses the word "MITM" and says the workload can see the substituted issuer.

**Alternatives.** Don't ship it. SNI and destination matching (`passthrough`)
cover "which hosts may I reach" without decrypting anything, and decrypting a
user's traffic is a serious thing for a scheduler to offer.

**Trade-off.** Legion ships a credential-visible interception path. Anyone who
enables it can read every secret their workloads send, and the feature's
existence invites use in places where it is not defensible.

**Why.** "Granular filtering of the context that egresses" is one of the three
things the feature was asked for, and it is **unachievable without inspection** —
an approved destination is not an approved payload, and the payload is inside
TLS. The honest options were to ship it with the safety rails visible or to
ship a feature that quietly cannot do what it claims. It is defensible here
specifically because a sandbox's root filesystem is one Legion built, for code
the operator is already sandboxing — and indefensible almost everywhere else,
which is why the gate is a cluster-level operator decision and not a jobspec
field.
