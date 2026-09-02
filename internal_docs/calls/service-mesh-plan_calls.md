# Judgement Calls: The Built-in Service Mesh (unified plan)

Decisions taken while re-examining the service-discovery mesh follow-on and
unifying it with the network handlers plan
([`plans/service-mesh-plan.md`](../plans/service-mesh-plan.md)). Calls that
belong to policy, capture, and handlers specifically live in
[`network-handlers-plan_calls.md`](network-handlers-plan_calls.md); these are
the calls that changed the *mesh* design.

**Entries are ranked by project impact, highest first; an entry's number is its
rank.**

---

## 1. DNS resolves a service to the task's upstream VIP, never to raw endpoints

**Call.** `<service>.svc.legion` answers with a per-allocation VIP in
`127.0.0.0/8` on which `netd` listens on the service's canonical port. No A
record ever names a backend node.

```
db.svc.legion  →  127.42.0.3   (netd listens on 127.42.0.3:5432)
```

**Alternatives.** The service-discovery plan's design: A records of every
healthy endpoint, randomized, capped, TC-bit + TCP fallback for large sets,
VIP "recommended" for load balancing.

**Trade-off.** DNS-direct is gone as a consumption mode: a client that wants
the raw endpoint list uses `legion service resolve` or the API. A VIP is
per-allocation state on the node. macOS cannot do 127/8 VIPs without root
`ifconfig lo0 alias`, so it keeps loopback ports and an SRV answer — two
platform behaviors to document.

**Why (simplicity, and it would not have worked).** Legion services live on
dynamic host ports. An A record carries no port, so "resolve `db` by DNS and
connect" was never going to reach a dynamic-port service without SRV support
that almost no client has. The critique had already concluded VIP-first (B3)
and spent two more rows (B1, B2) mitigating DNS-direct's failure modes. Making
the VIP the *only* answer deletes all of that machinery, makes `db.svc.legion:5432`
work in any URL or config file, and keeps balancing where it is health-aware
— in the forwarder.

---

## 2. mTLS is on node identity with attributed workload identity, not per-workload certificates

**Call.** The manager runs a **node CA**. `netd`↔`netd` hops are mTLS on node
certs (SAN = `{region}:node:{pool}`). The sending `netd` attributes the
workload in a PROXY v2 TLV; the receiving `netd` trusts the node peer and
enforces intentions on the attribution. Per-workload certs are an on-demand
feature for tasks that do their own TLS.

**Alternatives.** The original plan: a workload CA issuing a short-lived cert
per allocation, rotated, presented by the forwarder on the workload's behalf.
Cryptographically stronger — the identity is proven by the cert, not asserted
by a trusted node.

**Trade-off.** A compromised node can misattribute any workload on it. That is
also true of the per-workload design (the node's forwarder holds every
workload's key), so the *practical* trust boundary is the same; the
*cryptographic* one is weaker and the doc says so.

**Why (performance, then simplicity).** The critique named the cert-rotation
storm (A5) as a real risk: N×allocations short-lived certs is the Istio
problem. N nodes is not. It is also ztunnel's model, which the industry
arrived at for the same reason. And one CA that issues node certs closes the
long-standing "worker mTLS / per-node credentials" TODO — raft transport auth
gets the same certificates — so the mesh's identity work pays for the control
plane's too.

---

## 3. There is no waypoint; L7 traffic management is `connect.upstreams[].http`

**Call.** Retries, timeouts, canary split, and outlier ejection are options on
an upstream, executed by `netd`'s HTTP mode on that upstream's listener. No
separate per-service proxy, no plugin.

**Alternatives.** The SD plan's waypoint: an Envoy/pingora datapath plugin
attached per service identity, so L7 cost is paid only by services that ask.

**Trade-off.** No Envoy feature parity, ever, by this route — `netd`'s HTTP
implementation is the ceiling. No per-*service* L7 policy set by the service
owner; it is per-*consumer*, set on the upstream. (Ingress `rules` cover the
service owner's side.)

**Why (simplicity).** The waypoint's justification was "L7 is opt-in." An
`http` block on an upstream is also opt-in, at the same granularity, and it is
one datapath to operate, benchmark, and secure instead of two. Legion's claim
is the simplest scheduler to run; a second proxy with its own lifecycle,
plugin ABI, and upgrade story would have been the mesh doing exactly what the
plan's own tenets warned against.

---

## 4. `capture` becomes a value of `network.mode` — ↩ changes the handlers plan

**Call.** The handlers plan's `network.capture = "gated" | "proxyEnv"` is
replaced by `network.mode = "host" | "gated" | "auto"`, with `bridge` and
`cni/<name>` reserved.

**Alternatives.** Keep `capture` as a separate key so `mode` stays free for
the datapath plugin.

**Trade-off.** `mode` now means "which datapath" and `gated` is a datapath
that assigns no IP, which is a slightly odd member of a list that will one
day include `bridge`.

**Why (simplicity).** `mode` has been parsed, stored, and read by nothing
since service-discovery v1, reserved for precisely this — selecting how a
task's network is built. Two fields that both answer "how is this task
wired" would be the config-ordering footgun in miniature. `gated` *is* a
datapath: loopback plus `netd`.

---

## 5. The datapath plugin stays deferred; gated namespaces are the isolation story

**Call.** No `DatapathPlugin` ABI, no bridge, no CNI shim in this plan.

**Alternatives.** Build it, as the SD plan's phase 4 — it is the item every
comparison with Nomad/K8s raises.

**Trade-off.** No per-allocation routable IP, so a workload that must be
reached on arbitrary ports by IP, or must do raw UDP/ICMP, is not served. The
CNI ecosystem stays outside.

**Why (simplicity, and the buyer).** Everything in §1 of the plan — find,
reach, control, see — is delivered by VIPs, gated namespaces, and `netd`
without IPAM. Per-allocation IPs serve a different buyer (lift-and-shift of
IP-centric software) and carry a platform-specific subsystem with them. The
seam is kept open in `network.mode`; the work is not started.

---

## 6. Drain-before-stop is an ordering change, included; self-preservation is deferred

**Call.** M6 marks an allocation draining before `SIGTERM`, withdrawing it
from the feed and letting connections finish. The catalog's self-preservation
on correlated failure is left for the discovery side.

**Alternatives.** Do both here (they were listed together), or neither.

**Trade-off.** A zone-wide health blip still mass-evicts from the feed; the
forwarder fails static only on a *manager* outage, not on a correlated
*health* failure.

**Why.** Drain needs no new machinery — the per-node forwarder already
outlives the app, so it is purely "stop routing, wait, then stop." Self-
preservation is a detector-trust policy on the catalog projection, with no
datapath component; putting it in a mesh plan would be scope for its own sake.

---

## 7. DNS-SD metadata records are dropped

**Call.** No `_services.legion` PTR enumeration and no `meta.<svc>` TXT.

**Alternatives.** Ship them — cheap once a resolver exists.

**Trade-off.** No metadata-over-DNS for clients that cannot call the API.

**Why (simplicity).** `legion service list`, `resolve`, the MCP tools, and the
UI already answer "what services exist and what are they tagged." A resolver
is a security-sensitive component; every record type it serves is surface to
get right. Nothing asked for this.

---

## 8. Drain is worker-side timing keyed to the heartbeat interval, not a manager state

**Call.** The manager keeps doing what it does — delete the allocation row,
which withdraws the endpoint from every node's feed on that node's next
heartbeat. The *worker*, on learning the allocation is undesired, moves it
out of the live set and holds the signal for one heartbeat interval (or
`stop.drain`) before the ladder. Only tasks with a `service` block drain;
`"0s"` opts out; `5m` caps it.

```nickel
stop = { drain = "5s" }   # default: one heartbeat interval of the node
```

**Alternatives.** A `draining` allocation status in the store with a
manager-side timer that deletes the row later (a new state, a new sweep, a
new thing to reconcile after failover); or consumer-side retry on refused
connections in the forwarder (hides the symptom, costs a round trip per
rollout step, and does nothing for direct dialers).

**Trade-off.** The window is a bound, not an acknowledgement — a node whose
heartbeat is late learns late, and a connection it opens in that gap is still
refused. Nothing is persisted: a worker restart mid-drain signals at restore,
which is what grace already does. Every rollout step of a service task is one
interval slower.

**Why.** No new state, RPC, or failure mode; the read model already removes
the endpoint on the same heartbeat that removes the allocation, so the only
missing piece was the order on the worker. Simplicity, and the plan's own
framing (§3.6: "an ordering change, not new machinery").

---

## 9. The datapath owns the advertised port; the task binds a private twin

**Call.** With an ingress chain, the advertised port — `LEGION_PORT_<L>` as
consumers and the catalog see it — is bound by the datapath on the host, and
the task is handed a second, private port from the same dynamic range as its
own `LEGION_PORT_<L>`. Nothing in the task changes; the task cannot take the
advertised port back because the bind fails.

**Alternatives.** Transparent redirect (iptables `REDIRECT` / eBPF) to a
datapath port — kernel-dependent, root-only, absent on macOS. Or ask the task
to bind a documented private port itself — a jobspec change per task, and
nothing stops it binding the advertised one anyway.

**Trade-off.** One more port per advertised port from the dynamic range; the
task sees the datapath as its peer (attribution is in the access log until
PROXY v2 lands with external handlers); health checks traverse the chain, so
a deny-all ingress denies its own checker (`net doctor` warns, the doc says
to allow `127.0.0.0/8`).

**Why.** Enforceable everywhere with one mechanism and zero jobspec change —
name-is-config applied to inbound. Simplicity and extensibility (the same
listener is where PROXY v2, mTLS termination, and wake-on-request attach).
