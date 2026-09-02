# Judgement Calls: Network Handlers

Decisions taken while designing
[`plans/network-handlers-plan.md`](../plans/network-handlers-plan.md). The plan
is **PLANNED, not implemented** — these are design-time calls. Revision 2 of
the plan reversed six of revision 1's calls; each reversal is marked
**↩ reversed** with what the first version got wrong, because a reversal is the
entry most worth reading.

**Entries are ranked by project impact, highest first; an entry's number is its
rank.** Insert new entries at their rank and renumber rather than appending.

---

## 1. `gated` capture is a property of the network namespace, not a sandbox feature — ↩ reversed

**Call.** Unbypassable capture is defined as *"the workload's network namespace
contains only loopback and a listener Legion owns."* `sandbox` implements it
with no guest NIC and vsock; `container` and `exec` implement it with an empty
network namespace that `netd` creates, binds into, and the workload joins.
Enforced for all three on Linux. `netd` implements the existing
`TaskPlugin::network_gate()` hook for gated allocations as *policy withheld*,
which makes edge-boot's `boot` gate a real boundary on every runtime.

```nickel
network = { capture = "gated" },   # default for sandbox; opt-in for container/exec
```

**What revision 1 said.** Unbypassable capture was sandbox-only, built on
vsock, and the honesty table listed `container` and `exec` egress as
permanently cooperative because "Linux containers run in the host netns."

**Alternatives.** (a) Revision 1's shape — vsock is the feature, other
runtimes wait for the datapath/CNI plugin. (b) Full netns-per-allocation with
veth/bridge/IPAM — the datapath plugin, done properly.

**Trade-off.** `netd` now needs `CAP_SYS_ADMIN` on Linux to create namespaces
and `setns` into them — the same privilege the worker already exercises for
`exec` hardening's mount namespace and for running `youki`, so no *new*
privilege, but a new place it is used. Inside a gated namespace v1 capture is
proxy-env, not transparent: a tool that ignores `HTTP_PROXY` has no route and
fails. macOS gets none of it.

**Why (simplicity, and it is a security boundary).** The first version
confused the transport with the property. Once the property is "an absent
device," every runtime that can have a namespace can have the property, the
public doc's honesty table flips two rows from *Yes, bypassable* to *No*, and
there is one concept to explain instead of "sandboxes are special." It also
costs almost nothing: the OCI bundle already declares four namespaces and
omits this one (`oci_bundle.rs:576`); adding one with a `path` is the whole
container change. And it closes a gap edge-boot documented honestly — "a
`boot`-gated `exec` task can still reach the network through the host's own
stack" — without that plan changing at all. (b) remains the right long-term
datapath and is not a prerequisite — an *empty* namespace needs no IPAM, no
bridge, and no plan.

---

## 2. Policy is not a handler; connect-time rules never touch the byte path — ↩ reversed

**Call.** `rules` is a separate key from `handlers`. A rule is evaluated at the
earliest point that can decide it — host/port/SNI/identity at connect or DNS
resolution, method/path/header at the HTTP request head, body patterns only
inside a `dlp` handler with a byte cap. A chain with only connect-time rules
never leaves L4: allow → `splice`, deny → close, nothing parsed.

**What revision 1 said.** `{ handler = "rules", rules = [...] }` — rules were
a hop in the chain like any other, which put every rule set on the byte path
whether or not it needed bytes.

**Alternatives.** Keep rules as a handler; it is one concept fewer in the
jobspec and the chain is uniform.

**Trade-off.** Two keys instead of one, and a user has to learn that `rules`
and `handlers` are different kinds of thing. Some rules (a path match) still
force L7, so the cost is not uniformly zero — the doc has to say which rules
cost what.

**Why (performance, measured).** Tensorlake's attribution: removing L7
interpretation from a proxy took throughput from 1.12 to 2.07 GB/s and CPU from
0.90 to 0.50 s/GB; kTLS + `splice` on top added only 2.07 → 2.50. So the L7
hop is ~2× throughput and ~45% CPU, and the single most valuable property the
datapath can have is *not entering L7 unless a rule needs it*. "Deny
everything but three hosts" is the sandbox default and is decidable at
connect time; making it pay the L7 tax because rules were modeled as a filter
would have thrown the whole gain away on day one. This also gives a second,
independent argument for entry 11 (never infer `protocol`).

---

## 3. External upstreams replace TLS interception as the way to see egress bodies — ↩ reversed

**Call.** `connect.upstreams` gains external destinations:
`{ name = "openai", to = "https://api.openai.com", handlers = [...] }`. The
task gets `LEGION_UPSTREAM_OPENAI=127.0.0.1:<port>`, speaks plaintext to it,
and `netd` originates TLS. The body is visible on the host because the
workload was pointed at a gateway. TLS `inspect` (MITM) is gone from every
phase.

```sh
# in the sandbox
export OPENAI_BASE_URL="http://$LEGION_UPSTREAM_OPENAI/v1"
```

**What revision 1 said.** `egress.tls.mode = "inspect"` — Legion terminates
TLS with a cluster CA injected into the sandbox trust store, runs the chain on
plaintext, re-encrypts. Gated behind `allowTlsInspect`, sandbox-only, called
"MITM" in the doc, and defended as "the only way to filter what egresses over
HTTPS."

**Alternatives.** Keep `inspect` alongside upstreams for apps that cannot be
pointed at a gateway (a hardcoded URL in a binary).

**Trade-off.** An app that cannot be re-pointed gets SNI/host-level control
only, not body filtering. That is a real gap, documented as one, and it is the
gap that would justify revisiting MITM — with a use case rather than in the
abstract.

**Why (simplicity, and the trust story).** The first version missed that
Legion already has the non-MITM shape: `connect.upstreams` is "dial a name
through a loopback port the forwarder owns," and an external name is the same
thing with a static target. Every AI-gateway product works this way for the
same reason. The result: Legion never holds a CA that can impersonate the
public internet, the recommended path serves the LLM use case that motivated
the feature, it works on every runtime and platform (no capture needed — it is
opt-in by address), and the plan loses a whole security-review-shaped section.
A design that needed a MITM to serve its second scenario was telling us
something.

---

## 4. Handlers are out-of-process — a webhook or a proxy, never a `cdylib`

**Call.** Two external tiers: `authz` (Legion POSTs metadata, receives a
verdict, caches it; no bytes) and `proxy` (any service or sidecar; PROXY v2 or
`Legion-*` headers; no SDK). Built-ins come from a closed `const HANDLERS`
table. There is no `plugins/nethandlers/` `cdylib` kind, now or planned.

**Alternatives.** (a) A `cdylib` kind, which is what every other extensible
surface in Legion is and which `docs/plugin-system.md` makes mechanical. (b)
WASM first, per the mesh doc's Part D.

**Trade-off.** A userspace hop per connection for `proxy`, one cached RPC per
new (identity, destination) for `authz`, and no compile-time typing of the
contract — hence `legion net handler test` in the same phase.

**Why (the failure domain, then simplicity).** A panic in an in-process
handler kills the node's datapath, and that datapath sees every workload's
traffic — arbitrary native code there is a cross-tenant exfil primitive. The
`async` datapath makes the dylib ABI worse. Out-of-process buys a crash
boundary, a resource boundary, any language, and zero new machinery: a handler
is a task, with images, health, restart, logs, ACLs, and rollouts already.
WASM is strictly better than `cdylib` and strictly more work than a service;
shipping the service tier first lets WASM be judged on demand.

---

## 5. `authz` is the first external handler, not `proxy` — new in revision 2

**Call.** The simplest way to run custom logic is a webhook:
`{ handler = "authz", url = "http://policy.internal/check", cache = "30s" }`.
`netd` sends JSON metadata (identity, labels, instance key, destination, HTTP
head in HTTP mode) and gets `{ verdict, reason?, ttl? }`.

**Alternatives.** Revision 1 had only `proxy`: every custom decision cost a
correct, streaming, upgrade-preserving HTTP proxy.

**Trade-off.** A second external contract to document and conformance-test.
A cached allow can outlive a policy change by up to its `ttl` (capped at 5m).

**Why (simplicity).** "Ask my service whether this is OK" is what most custom
logic actually is, and it is what Envoy `ext_authz`, OPA, and admission
webhooks all converged on. A 200/403 endpoint is an afternoon in any language;
a correct HTTP proxy is not. With caching it is also cheaper than a proxy hop —
one RPC per distinct (identity, destination), not per connection.

---

## 6. The datapath is `legion netd`, a separate process from the same binary — ↩ reversed

**Call.** Listeners, namespaces, vsock sockets, and the policy cache live in a
process the worker spawns and supervises, with fd handoff on restart. The
worker keeps control-plane duties. `netd` readiness gates gated task start;
`netd` health is node health.

**What revision 1 said.** Nothing — the chain lived in the worker process by
default, as the forwarder does today.

**Alternatives.** (a) Stay in-process and do listener fd handoff across worker
restarts — keeps one process. (b) Accept the connection loss and document it.

**Trade-off.** A second long-lived process per node to supervise, upgrade,
and reason about, against a codebase whose headline is one binary; an IPC
boundary between worker and datapath; macOS keeps the in-process path so there
are two arrangements to test.

**Why (it is a security boundary that must not blink).** Tasks already survive
a worker restart (`restore_allocations`, `task_manager.rs:2138`); the
forwarder does not (`daemon.rs:328`, `forwarder.rs:334` `Drop`). Today that is
a papercut. Under `gated`, the datapath is *the only network path* a workload
has — a worker upgrade would sever every sandbox on the node, and they would
keep running, healthy-looking, with a dead network. Tensorlake decoupled their
forwarder from the orchestrator for exactly this. (a) fixes restart but not
the crash boundary: an HTTP-parser panic would still take the heartbeat loop
with it. "One binary" survives because `netd` is a subcommand, not a second
install.

---

## 7. Cluster posture is a stored, selector-scoped policy object, not manager config — ↩ reversed

**Call.** `legion net policy apply -f …` writes a raft-replicated object with a
`selector` (`nodePools`, `namespaces`, `runtimes`, `jobs`, `labels`), a
`priority`, and `enforcement = "require" | "default"`. A task's inline
`network.egress` is the same object scoped to that task. Merge is
deterministic and every merged rule names its source. `rulesFrom = "kv://"`
is gone.

**What revision 1 said.** `network.defaultEgress` in `configs/manager.ncl`,
plus `rulesFrom = "kv://…"` on the task for dynamic rules.

**Alternatives.** Keep config for cluster-wide and add pool/namespace blocks
under it — no new table, no new RPCs.

**Trade-off.** A new table, six RPCs, a new `AclResource::Net`, and a CLI verb
family. A `require` policy applied to a pool with running jobs changes them on
the next heartbeat, which is powerful and therefore needs `apply --dry-run`.

**Why (correctness under raft, then simplicity).** With a manager group, a
config-file posture is whatever the current leader's file says — a drifted
file on one manager is a silent hole that appears after a failover. And it
cannot change without a restart, which defeats the 3am requirement. The ACL
system already solved exactly this shape; copying it is less new design than
inventing pool/namespace config blocks. Folding `rulesFrom` in was the
simplification: policy objects *are* the dynamic layer, so a second mechanism
for the same job was pure surface.

---

## 8. Policy is delivered content-addressed over the heartbeat, fail-static — refined

**Call.** `HeartbeatReply` carries `[{ allocation_id, policy_hash }]`; `netd`
fetches a missing hash once and caches it. Swap only after a successful
heartbeat; last-known-good on failure; explicit empty is authoritative; a
gated allocation with no policy yet has no network.

**Alternatives.** (a) Ship the merged rule set per allocation per heartbeat —
revision 1's implicit shape. (b) Add `WatchKv` and let workers subscribe.

**Trade-off.** One extra RPC on a cache miss, and up to one heartbeat (2s) of
staleness. Fail-closed-before-first-policy means a gated task's start waits on
the control plane.

**Why (performance, then failure semantics).** (a) is O(allocations) per
heartbeat; a node with a thousand sandboxes under one policy would ship the
same rules a thousand times every two seconds. Hashing makes it O(distinct
policies). (b) is a subsystem — a second broadcast channel, a streaming RPC,
subscription lifecycle — to save 2s, and a watch stream would have to reinvent
fail-static, last-known-good, and explicit-empty and would get them wrong the
first time. The heartbeat already has the right semantics for a security
control.

---

## 9. DNS is served by the datapath inside a gated namespace — new in revision 2

**Call.** `netd` (and the guest agent, in a VM) answers DNS on loopback.
Resolution happens on the host; connect-time rules are evaluated at
resolution; a denied host is `NXDOMAIN` plus a `net_denied` event; non-loopback
DNS has no route.

**Alternatives.** Leave DNS to the proxy (`socks5h://`, HTTP CONNECT by name)
and give the namespace no resolver.

**Trade-off.** A resolver to implement and keep correct (TTLs, negative
caching, search domains), inside a component that is supposed to be boring.

**Why (a security boundary the first version left open).** Revision 1 did not
mention DNS. A no-NIC sandbox cannot resolve anything, which breaks every
tool that resolves before connecting — and if it *could* reach a resolver,
DNS is the textbook exfiltration channel. Serving it on loopback closes the
channel by construction, makes denial one round trip instead of a hanging
connect, and is the cheapest place a rule can be decided (entry 2). It also
dovetails with the service-discovery plan's node-local DNS item.

---

## 10. Egress ships before ingress

**Call.** Phases 0–4 build egress; the core sandbox use case completes at
phase 4. Ingress is phase 5.

**Alternatives.** Ingress first — the order the request stated, and the easier
demo.

**Trade-off.** mTLS ingress and the autoscaler's wake-on-request wait longer.

**Why.** Every capability the request named — per-user observability,
dynamic endpoint lockdown, filtering what leaves — is egress. Ingress's
marquee scenario needs the CA anyway. Egress also has a seam
(`forwarder.rs:444`); ingress has none.

---

## 11. `protocol` is declared, never inferred from the shape of a rule

**Call.** `egress.protocol` defaults to `tcp`; a path in a `tcp` chain is a
submit-time error naming the fix.

**Alternatives.** Infer HTTP from a `/` in an `allow` entry — the one-liner
would cover both cases.

**Trade-off.** The most useful one-liner costs an extra line.

**Why (simplicity at 3am, and now performance).** Inference silently changes
the chain's security posture *and* — per entry 2 — its cost by ~2×. A rule
set that became L7 because someone typed a slash is a policy you cannot
review by reading and a bill you did not agree to.

---

## 12. Version skew is a placement constraint, not a best-effort ignore

**Call.** A manager refuses to place a captured or policied allocation on a
worker whose fingerprint lacks support, with a legible reason.

**Alternatives.** Ordinary proto forward-compat: the old worker ignores the
field and runs the task.

**Trade-off.** Chained jobs can be unplaceable mid-upgrade; the first thing in
Legion that gates placement on worker version.

**Why (security boundary).** The default behavior here is "quietly run the
untrusted workload with its policy removed, reporting healthy." An unplaced
allocation is a page; a silently unfiltered sandbox is an incident nobody
opens.

---

## 13. An ingress chain changes what `LEGION_PORT_<label>` contains, rather than adding a variable

**Call.** `LEGION_PORT_<label>` holds the private port the task binds; the
advertised port is what the catalog and checks target.

**Alternatives.** `LEGION_BIND_<label>` alongside — more explicit.

**Trade-off.** A task printing its own address prints the wrong one; health
checks traverse the chain, so deny-all ingress fails your own check.

**Why (simplicity).** "Bind `$LEGION_PORT_<label>`" stays true for every job,
chain or not; a service can gain an ingress chain without touching its image.
`doctor` names the health-check case.

---

## 14. mTLS ships bring-your-own-PKI first, a Legion workload CA second

**Call.** 6a: `certPath`/`keyPath`/`clientCa`. 6b: `certFrom = "legion"`.

**Alternatives.** CA first, only ever Legion-issued certs.

**Trade-off.** Two cert-source paths; for one phase, mTLS exists but
`service.allow` is still control-plane.

**Why.** There is no workload CA today (`src/certs.rs` is a dev fixture); 6b
is issuance, rotation, revocation, and a new root of trust. Every operator
with an internal CA can be served without any of it.

---

## 15. Attribution goes to events and logs, never to metric labels

**Call.** `network.labels`, instance key, and identity appear on every event
and access-log line and on no metric.

**Alternatives.** Label the metrics — what a dashboard wants.

**Trade-off.** "Requests per user" is a log query.

**Why (it would not have worked).** The registry caps a family at 256 label
combinations and folds overflow into `"other"` — a per-user label on a sandbox
fleet silently destroys the series on the first thousand users.

---

## 16. `gated` defaults on for `sandbox` and off for `container`/`exec`

**Call.** `capture = "gated"` is the default only where it is today's behavior
plus a proxy (`sandbox` already has no network by default). Containers and
exec keep host networking unless a task or a `require` policy says otherwise.

**Alternatives.** Default `gated` everywhere on Linux — the secure default.

**Trade-off.** Two runtimes stay cooperative until someone opts in; the
honesty table has to say "on Linux, if you enable it."

**Why (rollout).** A container that loses host networking on a Legion upgrade
is the kind of default change this repo does not make silently. The sandbox
operator gets fleet-wide `gated` with one `require` policy on the pool, which
is the intended path anyway.

---

## 17. The manager's policy-body cache is in memory with a recompute fallback, not a table

**Call.** `FetchNetPolicy(hash)` is served from an in-memory `hash → body`
map the heartbeat builder fills. On a miss (failover, restart, eviction) the
manager recomputes the node's desired set through the same merge and serves
the matching hash. Nothing about merged bodies is persisted.

**Alternatives.** Persist merged bodies in a `net_policy_body` table keyed by
hash, written by the heartbeat builder — always answerable, no recompute.

**Trade-off.** A cold manager pays one desired-set merge per first fetch of
each hash, and a body evicted under `NET_POLICY_CACHE_MAX` (4096) costs the
same. A table would also give a history of every merged policy that ever
ran, which the in-memory design does not.

**Why (simplicity, and it is derivable state).** The merge is deterministic
over stored policies plus the task's declared network block, both of which
are already persisted and replicated. Persisting the output would put a
write on the heartbeat path — the hottest path in the system — to store
something any manager can recompute in microseconds. The cache is a
shortcut for the common case, and correctness never depends on it.

---

## 18. External upstreams verify TLS against the Mozilla roots only, for now

**Call.** `to = "https://…"` / `tls://` originates TLS with `webpki-roots`
and no client certificate. A pinned CA (`tls.caPath`) is parsed and
validated on the chain but not yet honored on upstreams.

**Alternatives.** Ship the pin in the same phase — it is one
`RootCertStore` per upstream.

**Trade-off.** An internal service with a private CA cannot be an external
upstream until the mTLS phase; the workaround is the catalog (make it a
Legion service) or a `tcp://` upstream that lets the task do its own TLS.

**Why (phasing).** The mTLS phase owns certificate material — file paths,
`${artifact}` references, the node CA — and per-upstream trust belongs with
it rather than as a one-off that would be redone when that phase lands.
Public APIs, the LLM-gateway case that motivated external upstreams, are
served today.
