# Mesh control path: per-service endpoint deltas, cached policy, cheap mesh hops

> **Judgement calls:** [mesh-control-path-plan_calls.md](../calls/mesh-control-path-plan_calls.md)

**Status: SHIPPED** (planned and built 2026-11-08, one change for
[architecture review](../architecture-review-2026-11-07.md) findings NET1–NET9;
public pages: [service-discovery.md](../../docs/service-discovery.md#how-endpoints-reach-a-node),
[network-handlers.md](../../docs/network-handlers.md#sessions),
[cluster-ca.md](../../docs/cluster-ca.md#how-workers-learn)).
What shipped and what was deferred is listed per phase in
[Implementation phases](#8-implementation-phases); the deferred items are open
in [TODO.md](../TODO.md) under *Service discovery & the mesh* and *Networking*.
Deliberately deferred: pooled node-to-node tunnels (HBONE-style HTTP/2
CONNECT) and kTLS (NET3's fuller fix), reading guard counters over netlink
(NET6), interning endpoints on the dial path (NET8), and configurable node
port ranges (NET9).

## 1. Problem statement

The packet path of the mesh is sound. Its control path is not, and the review
found nine places where the cost grows with fleet size or connection rate:

- **NET1.** Every heartbeat from a node with `connect` upstreams rebuilds that
  node's endpoint feed from scratch: it re-parses every allocation's
  `task_connect`, runs one catalog query per (service, consuming job), parses
  three JSON values per catalog row, and copies the result into every
  allocation's list. One node with 100 consumers of a 1,000-endpoint service
  ships about 100k endpoints per beat, and the worker replaces every list
  wholesale. Convergence follows the heartbeat cadence, which is
  `ceil(N/1250)` s: a backend going down takes ~8 s to leave consumers' lists
  at 10k nodes and ~16 s at 20k.
- **NET2.** Past its gate, `build_net_policies` lists every stored policy,
  parses each allocation's `task_network`, and merges per allocation, on every
  heartbeat: O(policies × allocations) per node per beat whether or not
  anything changed.
- **NET3.** Each L4 mesh connection is its own mTLS connection, and the
  forwarder rebuilds the rustls client config on every identity change, which
  throws the resumption cache away (and each ingress mesh port has its own
  server config with its own cache, also dropped). Every 8-hour certificate
  refresh and every CA rotation is a burst of full handshakes.
- **NET4.** HTTP mode collects request bodies with no cap when no body hop
  applies. One large upload can exhaust the worker's memory, and every
  upload is delayed until it is fully read.
- **NET5.** IPv6 addresses are formatted `host:port` without brackets,
  `worker_address_host` splits on the last `:`, and ingress binds `0.0.0.0`.
  An IPv6-only node cannot take part in the mesh.
- **NET6.** The nftables guard fallback has no established-flow
  short-circuit, so every egress packet walks one rule per allocation, and its
  counters are read by forking `nft -j list table` every heartbeat.
- **NET7.** A CA rotation makes every node re-issue its certificate within one
  heartbeat: ~10k CSRs at once on an `exec` or Vault signer.
- **NET8.** The service-graph ledger is one node-wide `Mutex<HashMap>` keyed
  by owned strings, the forwarder's in-flight map allocates a `String` per
  connection, and each copy direction allocates a 16 KiB buffer.
- **NET9.** VIP blocks come from `127.42.b`, so a node holds at most 256
  allocations with upstreams; and the datapath's own ports (upstream loopback
  ports, ingress twin ports) share 20000–32000 with task ports.

## 2. Prior art

- **Envoy delta xDS** (incremental `DeltaDiscoveryRequest`): the client says
  which resources it holds at which version; the server sends only resources
  that changed, and removals by name. Taken: the client reports versions, the
  server sends changed sets. Rejected: xDS itself (a second protocol and an
  Envoy-shaped resource model for a mesh that has one client).
- **Kubernetes EndpointSlices**: endpoints of one service are split into
  slices so one backend's change does not rewrite every consumer's object, and
  kube-proxy applies per-slice changes. Taken: per-service sets, applied per
  service on the node. Not taken: slicing within a service (a set is one
  unit; see calls).
- **Consul blocking queries / `X-Consul-Index`**: a reader parks on an index
  and wakes on change. Taken: the work stream push wakes on change;
  the heartbeat stays as the reconciling backstop.
- **Nomad template/consul-template** re-render only on change. Legion's
  templates already did; they now read the per-service sets.
- **Istio ambient ztunnel** pools HBONE (HTTP/2 CONNECT) tunnels per node
  pair; **Linkerd** keeps a connection pool between proxies. Evaluated for
  NET3, deferred (calls #4).
- **rustls**: a client session is only resumed with the *same*
  `ServerCertVerifier` and `ResolvesClientCert` objects (`compatible_config`),
  and a server does not re-verify the client chain on resumption. That
  decides the NET3 design: hot-swappable certificate resolvers, configs built
  once per trust bundle, and an explicit age bound on resumed identities.
- **Envoy `per_request_buffer_limit_bytes`**: retries buffer the body up to a
  limit; past it the request is not retried. Taken for NET4 (calls #6).
- **Go `net.JoinHostPort`/`SplitHostPort`**: the canonical bracket rules for
  NET5.
- **cert-manager / SPIRE** renew with jitter before expiry; SPIRE agents
  rotate at half-life with jitter. Taken for NET7.

## 3. Jobs to be done, by persona

**Cluster operator**
- When my fleet grows past a few thousand nodes, I want a backend that stops
  answering to leave every caller's list within a second or two, so a
  failover does not wait out a heartbeat that gets slower as I add nodes.
- When I run many services with connect upstreams, I want the managers'
  heartbeat work not to grow with (endpoints × consumers), so adding a
  service does not slow liveness for the whole fleet.
- When I rotate the cluster CA, I want nodes to re-issue spread out over
  minutes instead of in one beat, so my external signer is not flooded.
- When I run nodes on IPv6-only networks, I want the mesh to work.

**Job author / app developer**
- When my service accepts uploads through the mesh in HTTP mode, I want large
  bodies to stream through, so a 1 GB upload does not sit in the worker's
  memory and does not break the node.
- When I configure retries, I want small requests retried and large ones sent
  once rather than refused, and a knob to raise the limit.

**On-call responder**
- When I see mesh latency spikes every eight hours, I want to know they are
  not handshake storms from certificate refreshes.
- When a node is removed, I want its identity to stop working at certificate
  expiry even if it held resumable sessions.

**Security / platform admin**
- When a CA is removed from the trust bundle, I want no session established
  under it to keep working.

*Not in scope:* the autonomous agent persona. Nothing here changes an
agent-facing surface (CLI, MCP, gRPC user API); the protocol changes are
worker-plane.

### User scenarios

1. A 10k-node cluster with a 200-endpoint `db` service and 3,000 consuming
   allocations. A `db` backend's health check fails. Its node's heartbeat
   records the health change; the managers' changeset projection marks `db`
   dirty; the refresher recomputes `db` once and pushes the new set on the
   work stream of every node that consumes it. Consumers stop dialing the
   backend in well under a second, and no heartbeat in between reads the
   catalog.
2. An operator runs `legion net ca rotate` on a 20k-node cluster. Nodes
   re-issue over ~7 minutes (deterministic per node), the signer sees ~50
   CSRs/s, and the mesh sees full handshakes only from the trust-bundle
   change, not once per refresh.
3. A task uploads a 2 GiB file to `https://storage.internal` through an
   HTTP-mode upstream with retries on. The first 1 MiB is buffered, then the
   request streams and is not retried; the worker's memory stays flat.

## 4. Goals and non-goals

Goals:
- Heartbeats from nodes whose upstream services did not change read nothing
  from the catalog and ship no endpoints (NET1).
- Endpoint changes reach streaming workers promptly, independent of cadence.
- Policy assignments recompute only when an input changed (NET2).
- Mesh connections resume across leaf refreshes; no session outlives its
  certificates or a trust-bundle change (NET3).
- HTTP mode never buffers an unbounded body (NET4).
- IPv6 endpoints, workers, and ingress work (NET5).
- NET6–NET9 fixed where simple and safe.

Non-goals:
- A new endpoint resource model (slicing within a service, xDS).
- Pooled multiplexed mesh tunnels and kTLS (evaluated, deferred; calls #4).
- Changing intention semantics, the catalog query, or `ResolveService`.
- Making the node port ranges configurable (NET9; calls #10).

## 5. Background — what exists today

- Endpoint feed: `build_upstream_endpoints` (`src/manager/server.rs`, called
  from the heartbeat handler) and `build_handler_endpoints` for `proxy`
  handler targets; catalog rows from `Store::service_endpoints`
  (`src/storage/store.rs`), intentions applied per consumer with
  `allow_permits`. Wire: `HeartbeatReply.upstream_endpoints`, one
  `UpstreamEndpoints` per (allocation, upstream). Worker: `Netd::apply_desired`
  (`src/worker/netd/datapath.rs`) calls `set_endpoints` for every entry;
  `apply_templates` (`src/worker/daemon.rs`) renders from the same list.
- The read model and the work log follow committed changesets on every
  replica (`Store::project_changeset`), which is how followers stay coherent;
  `local_disk` mode projects only `READ_MODEL_TABLES` (no `allocation_health`).
- `StreamWork` (`src/manager/server/work_stream.rs`,
  `src/storage/work_log.rs`) already pushes checkpoint work as a hint outside
  the sequenced set, with the heartbeat as the record.
- Policy: `build_net_policies` with a TTL-cached generation
  (`net_policy_generation_cached`), merged bodies cached by hash
  (`remember_net_policy`).
- TLS: `MeshIdentity::client_config`/`server_config` (`src/worker/netd/tls.rs`),
  `ForwarderContext::set_mesh_identity` and `ingress_server_config`
  (`src/worker/forwarder.rs`).
- HTTP mode: `src/worker/netd/http.rs` (`body.collect()` in the request path,
  `attempt_request` replays a buffered body).
- Kernel guard: `src/worker/netd/kernel_guard.rs` (`render`, `read_counters`).
- Identity refresh: `refresh_node_identity` (`src/worker/daemon.rs`),
  `NodeIdentity::needs_refresh` (`src/worker/plane.rs`).

## 6. Proposed design

### 6.1 Versioned endpoint sets per service (NET1)

**Manager.** An `EndpointIndex` per manager process
(`src/storage/endpoint_index.rs`) holds, per canonical service reference
(`web`, `payments-prod/web`), the service's catalog rows sorted by allocation,
a content digest, and a version from a process-wide monotonic counter. Per
consuming identity it memoizes the intention-filtered, healthy-only wire list
and its digest; consumers no intention restricts share one list.

The index is invalidated from committed state: the store's changeset
projection (every replica, raft and `local_disk`) marks a cached service dirty
when an allocation of it is inserted, moved, updated, or removed, its health
changed, its node's liveness or advertised address changed, or its task
config changed. `local_disk` mode, whose projection does not see
`allocation_health`, marks after the health write commits. A rebuild of
node-local state marks everything dirty. Every cached set is also recomputed
when older than 30 s, as a backstop.

A refresher task recomputes dirty services that some node consumes (one
indexed query per service, shared by every node), bumps the version only
when the content changed, and wakes the work streams of the nodes that
consume it.

**Per node.** The heartbeat computes the node's *bindings* — (allocation,
upstream key, handler?) → (service, consumer) — from memoized parses of
`task_connect` and cached policy bodies, deduplicates them into the node's
distinct sets, and compares each set's digest with what the worker says it
holds. The reply carries only sets whose digest differs, and the bindings
only when their digest differs. A steady beat reads nothing from the store
and ships no endpoints.

**Push.** When a consumed service's version moves, the node's `StreamWork`
stream sends a push-only message carrying the changed sets
(`StreamWorkReply.push_only`, `endpoint_feed`). It takes no sequence number,
is never replayed, and the heartbeat stays the record: a lost push costs one
heartbeat interval.

**Worker.** `EndpointFeedState` (`src/worker/netd/endpoint_feed.rs`) holds the
sets and bindings, applies a set only when it is newer (same manager epoch
and higher version, or another epoch), and hands each listener a shared list
that is swapped only when its set changed. Failure keeps the last known good
set (fail-static, unchanged). Templates read the same state.

Wire surface (worker plane only):

```proto
message HeartbeatRequest { ... EndpointFeedState endpoint_feed = 22; }
message HeartbeatReply   { reserved 4; ... EndpointFeed endpoint_feed = 25; }
message StreamWorkReply  { ... EndpointFeed endpoint_feed = 8; bool push_only = 9; }
```

### 6.2 Memoized policy assignments (NET2)

`NetPolicyMemo` (`src/manager/net_policy_memo.rs`) keeps the stored policy
list per generation and the effective result per (generation, node pool when
a policy selects on pools, runtime, job, task network, task service, placed
network). A beat costs one hash per allocation; listing, parsing, and merging
happen once per distinct input and generation. The generation still comes
from the TTL-cached read, so followers stay coherent.

### 6.3 Mesh TLS that survives refreshes (NET3)

- One mesh client config and one mesh server config per trust bundle, each
  with a hot-swappable certificate resolver: a leaf refresh swaps the key in
  place, so sessions survive; a trust-bundle change builds new configs with
  new, empty session stores, so no session established under the old bundle
  resumes on either end.
- Mesh hops are TLS 1.3 only with stateful tickets; one node-wide server
  session store serves every mesh port.
- A resumed session reuses the identity of the handshake that started its
  chain. The client tags each ticket with the oldest leaf its chain may carry
  and drops tickets whose leaf expires within 30 minutes; both ends refuse a
  resumed session whose peer leaf has expired.
- Pooled tunnels and kTLS: deferred (calls #4).

### 6.4 HTTP request bodies (NET4)

| Situation | Body |
| --- | --- |
| A body hop (`dlp`, a plugin `body` stage) | Buffered up to the hop's `maxBytes`; over it, `403` (fail closed) or `413` (`onError = "allow"`), unchanged |
| Retries enabled for the method | Buffered up to `http.retries.maxBodyBytes` (default `1Mi`, at most `64Mi`); over it, the request streams and is not retried |
| Otherwise | Streamed |

```nickel
connect.upstreams = [{ to = "https://storage.internal",
  http = { retries = { attempts = 2, maxBodyBytes = "4Mi" } } }]
```

### 6.5 IPv6 (NET5)

`crate::structs::hostport::{join, split}` (Go's rules) everywhere an address
and a port meet: endpoint `addr`, external targets, `worker_address_host`,
templates, the egress proxy's CONNECT target, `TestNetPolicy` targets.
Ingress with no explicit bind binds `[::]` with `IPV6_V6ONLY` off (one socket,
both families) and falls back to `0.0.0.0` when the host has no IPv6.

### 6.6 nftables guard (NET6)

`ct state established,related accept` first in the output chain; the
per-allocation dispatch is one verdict-map lookup on the socket's cgroup
instead of one rule per allocation; counters are read at most every
`hostGuard.counterInterval` (default `10s`).

### 6.7 Re-issue splay on CA rotation (NET7)

A trust-bundle change re-issues after a deterministic per-node delay in
`[0, window)`, `window = min(25 × heartbeat interval, 10 min)` — 50 s up to
2,500 nodes, ~7 min at 20k. Immediate when the node's own issuer left the
bundle, when it holds no certificate, or when its certificate is due.

### 6.8 Per-connection costs (NET8)

Sharded edge ledger with borrowed-key lookups (no allocation on a hit);
in-flight counts keyed by shared `Arc<str>` with no per-connection
allocation; copy buffers from a per-thread pool.

### 6.9 VIP blocks and ports (NET9)

VIP blocks move to `127.64.0.0/10` (16,384 blocks per node, deterministic,
first free). The datapath's own node ports — upstream loopback ports, ingress
twin ports, canonical ports for undeclared services, confined egress proxies
— come from `61000–65535`, apart from task ports (`20000–32000`). The BPF
classifier and the nftables guard match the new range.

## 7. Proto, struct, config, and persistence changes

- Proto: `EndpointFeedState`, `EndpointSetVersion`, `EndpointFeed`,
  `EndpointBinding`, `EndpointSet`; `HeartbeatRequest.endpoint_feed`,
  `HeartbeatReply.endpoint_feed` (field 4 `upstream_endpoints` reserved),
  `StreamWorkReply.endpoint_feed`, `StreamWorkReply.push_only`.
- Jobspec: `connect.upstreams[].http.retries.maxBodyBytes` (in
  `configs/jobs/maximal.ncl`).
- Worker config: `hostGuard.counterInterval`.
- Persistence: the worker's datapath state file stores the endpoint feed
  (sets and bindings) instead of per-allocation lists. No schema migration.
- BPF object rebuilt for the VIP range.

## 8. Implementation phases

Each phase ships on its own; all shipped in one change.

1. **NET1** endpoint index, feed, push, worker state — shipped.
2. **NET2** policy memo — shipped.
3. **NET3** resolvers, shared stores, age bound — shipped; tunnels and kTLS
   deferred.
4. **NET4** streaming and replay cap — shipped.
5. **NET5** host:port helpers and dual-stack ingress — shipped.
6. **NET6** established short-circuit, vmap dispatch, counter interval —
   shipped; netlink counters deferred.
7. **NET7** splay — shipped.
8. **NET8** ledger, in-flight, buffers — shipped; interning endpoints on the
   dial path deferred.
9. **NET9** VIP range and datapath port range — shipped; configurable ranges
   deferred.

## 9. Testing plan

- Index: a node sees only changed services, versions advance only on content
  change, removal propagates, invalidation from projected changesets
  (allocation insert, health change, node lost), the age backstop.
- Feed: steady beats ship nothing; bindings digest; stale pushes ignored.
- Policy memo: invalidated by generation, task network, pool.
- TLS: a resumed session across a leaf swap; a flush on bundle change;
  ticket age bound.
- HTTP: `413` from a body hop, streaming with no retry, buffered replay under
  the cap, streaming past the cap.
- IPv6 join/split, endpoint formatting, `worker_address_host`, dual-stack bind.
- Guard: rendered-table tests and `nft -c` on the new table; counter interval.
- Splay: deterministic, bounded, immediate cases.
- VIP and port ranges: assignment tests.

## 10. Rollout and backward compatibility

Legion has no installed base, so nothing is shimmed. Breaking:
- `HeartbeatReply.upstream_endpoints` is gone; workers and managers must run
  the same build.
- VIP blocks move from `127.42.b` to `127.64.0.0/10`; datapath ports move
  to `61000–65535`. Placed allocations keep their recorded addresses until
  they are replaced.
- Mesh hops require TLS 1.3 (both ends are Legion nodes).
- A retried HTTP request whose body is over `maxBodyBytes` is no longer
  retried.

## 11. Open questions

None open; resolved questions are calls in the calls doc.

## 12. Acceptance criteria

- A heartbeat whose node's sets are unchanged carries no `EndpointSet` and no
  bindings (unit test on the feed builder).
- A health change on a consumed service reaches a streaming worker as a
  push-only message without a heartbeat (integration test).
- `build_net_policies` performs no store reads on a repeated beat (unit test).
- A mesh connection resumes after a leaf swap; not after a bundle change
  (unit test).
- A body over a hop's cap answers `413`; a large body without retries
  streams (unit tests).
- `[::1]:8080` endpoints dial; ingress binds dual-stack (unit tests).
