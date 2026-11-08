# Judgement calls: mesh control path

Calls made building [mesh-control-path-plan.md](../plans/mesh-control-path-plan.md)
(architecture review findings NET1–NET9). **Ranked by project impact,
highest first; an entry's number is its rank.**

## 1. Endpoint sets are soft state per manager, invalidated from committed changesets, delivered as heartbeat deltas plus stream pushes

**Call.** Each manager keeps an in-memory `EndpointIndex`: per service, the
catalog rows, a content digest, and a version from a per-process counter.
The store's changeset projection (every replica) marks a service dirty when
an allocation of it, its health, its node's liveness or address, or a task
config changes; a refresher re-reads dirty services that some node consumes
and wakes those nodes' work streams. The heartbeat compares the worker's
held digests with the index and ships only what differs; streaming workers
also get push-only `StreamWorkReply` messages. A 30-second re-read backstops
any invalidation the projection cannot name.

```proto
message HeartbeatRequest { EndpointFeedState endpoint_feed = 22; }   // what I hold
message HeartbeatReply   { reserved 4; EndpointFeed endpoint_feed = 25; } // what changed
message StreamWorkReply  { EndpointFeed endpoint_feed = 8; bool push_only = 9; }
```

**Alternatives.** (a) A replicated per-service version row, bumped on every
endpoint change, so versions are comparable across managers — what an
etcd-backed design would do. (b) A pure push protocol with the manager
tracking what each worker holds (like the work log's `held`), no heartbeat
reconciliation. (c) Heartbeat deltas only, no push.

**Trade-off.** Versions mean nothing across managers, so the protocol
compares content digests and orders only within an epoch; a worker that
streams from one manager and heartbeats another can flap between their views
while one replica lags (bounded by the next beat). Per-node interest lives in
memory on whichever manager the node talks to and is rebuilt from the next
heartbeat after a failover. In `local_disk` mode the projection does not see
`allocation_health`, so the health write marks the index itself; a write path
that bypasses both would be caught only by the 30-second backstop.

**Why (performance, simplicity).** (a) turns every health flip into a raft
write on a lane that is already the bottleneck (review SCH1) to save a digest
comparison. (b) makes the manager the record of what every worker holds and
needs a resync story per failover; the heartbeat already is the record, and
checkpoint pushes already ride the stream as hints, so endpoint pushes
follow the same pattern and a lost push costs one beat. (c) leaves
convergence on heartbeat cadence, which is the user's complaint at 10k+
nodes. The projection is how the read model and the work log already stay
coherent on followers, so the index gets follower coherence for free.

## 2. The wire carries per-(service, consumer) sets and listener bindings, not per-allocation lists

**Call.** `HeartbeatReply.upstream_endpoints` (field 4, one full list per
allocation per upstream) is removed with no fallback. The feed is two halves:
`EndpointBinding` (allocation, upstream key, handler?) → (service, consumer),
sent only when its digest changes, and `EndpointSet` per distinct (service,
consumer) on the node. Forty allocations of one job using `db` share one set.

**Alternatives.** Keep `UpstreamEndpoints` per allocation and send only the
entries whose list changed; or let the worker derive bindings from its own
`task_connect` and policy bodies.

**Trade-off.** Workers and managers must run the same build (no installed
base, so no shim, per AGENTS.md). The worker carries a small state machine
(`netd::endpoint_feed`) and persists sets and bindings instead of lists.

**Why (performance, simplicity).** Per-allocation deltas still ship N copies
of a changed 1,000-endpoint set to a node with N consumers. Worker-derived
bindings would duplicate the namespace qualification and intention-consumer
rules on both sides, and a node would be trusted to name which consumers it
asks for; the manager computing bindings keeps intentions enforced by the
manager.

## 3. Mesh TLS resumes across certificate refreshes, flushes on a trust-bundle change, and bounds a session chain by its certificates

**Call.** One client config and one server config per trust bundle, around a
`NodeCertResolver` whose key is swapped in place on re-issue; one node-wide
server session store; TLS 1.3 only. A different bundle builds new configs
with empty stores (every session dropped on both ends). The client tags each
ticket with the oldest of its own certificate generations its chain may
carry (a ticket inserted within 60 s of a take for the same peer inherits
that take's generation) and does not offer a ticket whose certificate
expires within 30 minutes; both ends refuse a resumed session whose peer
certificate has expired.

**Alternatives.** (a) Share one `ClientSessionMemoryCache` across rebuilt
configs, the review's suggestion — it does not work: rustls resumes only with
the same verifier and client-cert-resolver objects (`compatible_config`). (b)
Keep sessions across a bundle change too. (c) Flush on every refresh (today's
behavior, the finding). (d) Flush on a timer shorter than the certificate
lifetime.

**Trade-off.** The chain inference is conservative: a full handshake that
happens while a resumption to the same peer is in flight is labelled with the
older generation and is retired sooner than it needs to be (an extra full
handshake, never a weaker check). The expiry refusal is a backstop for a node
whose certificate stopped being re-issued; it fails that connection rather
than retrying it. TLS 1.2 is no longer offered on mesh hops.

**Why (security over performance).** rustls does not re-verify a peer chain
on resumption and re-issues tickets carrying the original identity, so
without a bound a removed node could keep resuming forever; its certificate
expiry is the mesh's revocation story (docs/cluster-ca.md). (b) would let a
session made under a CA that was removed from the bundle keep working. (d)
brings the burst back on its own schedule. Swapping the key keeps the 8-hour
refresh free, which is the finding.

## 4. Pooled node-to-node tunnels and kTLS are deferred

**Call.** NET3's fuller fix — HBONE-style pooled HTTP/2 CONNECT tunnels
between nodes, then kTLS — is not built. Each L4 mesh connection stays its
own TLS connection; resumption (#3) cuts its cost.

**Alternative.** Build the tunnels now over the `h2` crate already in the
tree.

**Trade-off.** A mesh connection still costs a TCP handshake and a TLS
resumption round trip, and TLS hops are still copied in user space.

**Why (simplicity).** Tunnels change the mesh wire format (CONNECT streams
with the workload identity per stream instead of a PROXY v2 TLV per
connection), need per-stream flow control and a drain when a tunnel's
certificates near expiry, and touch the ingress, the forwarder, HTTP mode's
pools and the gated proxy. That is its own plan (TODO, Networking), not a
phase of this one.

## 5. A retried request whose body is over the cap streams once instead of failing with 413

**Call.** With no body hop and no retry, the request body streams. With
retries, it is kept up to `http.retries.maxBodyBytes` (default `1Mi`, max
`64Mi`); a larger body streams and the request is not retried.

```nickel
http = { retries = { attempts = 2, methods = ["PUT"], maxBodyBytes = "8Mi" } }
```

**Alternative.** The review's direction: cap the replay buffer and answer
`413 Payload Too Large` past it.

**Trade-off.** A configured retry silently does not happen for a large body
(logged at `debug`). An operator who wanted "never send a large body without
a retry" has no way to say it.

**Why (ease).** Envoy behaves this way for its retry buffer: retries are an
optimization, and turning them on should not make large uploads fail. The
`413` stays where a body must be read whole: a body hop over its `maxBytes`.

## 6. Rotation splay is derived on the worker from the served cadence

**Call.** A trust-bundle change re-issues after `rotation_splay(node_id,
interval)`: a deterministic offset in `[0, min(25 × interval, 10 min))`,
where `interval` is the manager-served heartbeat floor. At once when the
node's issuer left the bundle, it holds no certificate, or it is due anyway.

**Alternatives.** (a) A new `HeartbeatReply` field carrying a window the
manager computes from its node count. (b) Random jitter per attempt.

**Trade-off.** Below 2,500 nodes the served cadence is the 2 s floor, so the
window is a constant 50 s rather than proportional to the fleet. File and
exec CA rotations now need the operator to wait out the window (at most
10 minutes) between "add the CA" and "switch the signer", where one heartbeat
used to do; the procedure in docs/cluster-ca.md says so.

**Why (simplicity).** The served cadence already grows with the fleet
(`ceil(N/1250)` s), so 25 beats keeps the signer near 50 CSRs/s at any size
with no new wire field. Deterministic offsets make a retry land at the same
time and spread the fleet evenly; random jitter re-rolls on every restart.

## 7. VIP blocks move to `127.64.0.0/10` and the datapath's ports to `61000–65535`, as constants

**Call.** 16,384 VIP blocks per node (`127.<64 + i/256>.<i%256>`, first free
wins), matched by the BPF classifier (object rebuilt) and the nftables guard.
Upstream loopback ports, ingress twins, canonical ports for undeclared
services and confined proxies come from `61000–65535`, apart from task ports
(`20000–32000`).

**Alternatives.** Make both ranges node configuration; keep `127.42` and
shrink blocks to /27; put datapath ports in `10000–19999`.

**Trade-off.** 4,536 datapath ports per node, in addition to (not instead
of) the task range; a host that widened `ip_local_port_range` past 61000 can
collide. Allocations placed before the change keep their recorded addresses
until replaced, and a node mixing both is covered by neither the old nor the
new guard rule for the old block (no installed base).

**Why (simplicity).** Placement assigns these on the manager, which would
need every node's ranges as attributes to make them configurable; a fixed,
documented range is explainable at 3 a.m. `10000–19999` holds well-known
host services (kubelet's 10250); above the default ephemeral range is the
quietest place. A /10 keeps one CIDR for the guard and the classifier.

## 8. The policy memo verifies its full inputs instead of trusting a hash

**Call.** `NetPolicyMemo` buckets results by a 64-bit hash of (pool when
selected on, runtime, job, task network, task service, placed network when
it says `gated`) and compares the stored inputs on every hit. The endpoint
feed's connect-parse memo does the same.

**Alternative.** Key on the hash alone.

**Trade-off.** Each entry holds copies of its input strings, and a hit costs
a string comparison.

**Why (security).** Job authors write `network` and `service`; the hash is
FNV, not collision resistant, and a crafted collision would hand one task
another's effective policy.

## 9. A `proxy` handler's endpoint set uses the allocation's job as consumer

**Call.** Handler bindings name `allocation.job_id` as the intentions
consumer, as upstreams do.

**Alternative.** Keep the old behavior: the workload subject
(`<region>:<job>:<group>:<task>`) was passed where a job id is expected.

**Trade-off.** A handler target's `service.allow` is now judged against the
job, which can open a target that was (accidentally) always denied before.

**Why.** `allow_permits` parses a job id; with the subject it read
`global:<job>:...` as a name in the default namespace, so an `allow` list
naming the job never matched and a namespaced caller qualified into the
wrong namespace. Using the job also lets handler and upstream bindings share
one set.

## 10. The nftables guard accepts established flows first and reads counters on an interval, not over netlink

**Call.** `ct state established,related accept` heads the output chain;
dispatch to an allocation chain is one `socket cgroupv2 ... vmap` per level;
counters are read at most every `hostGuard.counterInterval` (default `10s`).

**Alternatives.** Read counters over netlink (a new dependency); drop the
nftables path for eBPF only (an open decision in TODO).

**Trade-off.** A flow allowed before a policy change keeps going until it
ends; `net.denied` events from the guard arrive up to 10 s late.

**Why (performance, simplicity).** Judging at `connect()` is what the guard
is for; every stateful firewall short-circuits established flows. A rate
limit removes the per-beat fork without a dependency, and the netlink read
is left with the eBPF-only decision.

## 11. Ingress binds one dual-stack socket and canonicalizes mapped peers

**Call.** With no explicit bind, ingress binds `[::]` with `IPV6_V6ONLY`
off, falling back to `0.0.0.0`; accepted peers are canonicalized
(`::ffff:a.b.c.d` → `a.b.c.d`) so CIDR rules and logs see IPv4.

**Alternative.** Two sockets, one per family.

**Trade-off.** Depends on the kernel honoring `IPV6_V6ONLY = 0` (set
explicitly, not left to `net.ipv6.bindv6only`).

**Why (simplicity).** One listener per port keeps the ingress bookkeeping
unchanged.

## 12. Per-connection allocations: atomics, shards, and a buffer pool, not interned endpoints

**Call.** In-flight counts are `Arc<AtomicUsize>` values a connection holds
(no key, no lock on release); the edge ledger is 16 shards with borrowed-key
lookups; copy buffers come from a per-thread pool.

**Alternative.** Intern endpoint addresses (`Arc<str>`) through `Endpoint`
and the balancers.

**Trade-off.** The dial path still clones the picked `Endpoint` (its address,
node id, and tags) per connection.

**Why (simplicity).** `Endpoint` is used across the balancers, HTTP mode,
and templates; changing it is a larger diff for a cost the review itself
rated low until ~50k connections/s per node.

## 13. Oversized bodies at a body hop keep their status codes; the doc is corrected

**Call.** Over a body hop's `maxBytes`, a fail-closed hop still denies with
`403` and a fail-open hop still answers `413`. The public doc said a
fail-open hop lets the body through; it now says what the code does.

**Alternative.** Make a fail-open hop stream an oversized body through
unchecked, as the old doc promised.

**Trade-off.** `onError = "allow"` does not mean "allow" for size, which
reads oddly.

**Why.** Changing the meaning of a security hop's failure mode is out of this
change's scope; it is a TODO item instead.
