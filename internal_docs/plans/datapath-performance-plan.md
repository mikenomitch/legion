# Plan: Datapath Performance — Pooling, Splice, and Latency-Aware Balancing

> **Judgement calls:** [`calls/datapath-performance-plan_calls.md`](../calls/datapath-performance-plan_calls.md)

> **Status: SHIPPED** in [#42](https://github.com/mikenomitch/legion/pull/42).
> Phases 1–4, plus the per-byte metric fix found while measuring (§6.4).
> Results are in §12.
> Deferred, by name: kTLS for mesh hops (§6.6), multiplexed node-to-node
> tunnels for L4 mesh traffic (§6.7), and the `legion netd` process split,
> which stays with the [service-mesh plan](service-mesh-plan.md).

This plan follows the network-handlers plan's §7.12 performance invariants
("pinned, not promised") and a comparison against Istio, Linkerd, and Cilium.
It does not change what the datapath allows or denies. It changes what an
allowed flow costs.

---

## 1. Problem

Every connection a task makes through `connect` passes through the node's
forwarder, and every connection into a gated or fronted port passes through
the ingress listener. A loopback measurement of that path, mirroring the
forwarder exactly (accept, dial, `tokio::io::copy_bidirectional`), on Linux:

| | Direct | 1 hop | 2 hops |
|---|---|---|---|
| Request/reply on a kept-open connection, p50 | 6.5µs | 11.7µs | 15.9µs |
| New connection + request/reply, p50 | 39µs | 75µs | 117µs |
| Single-stream throughput | 4.3 GB/s | 1.9 GB/s | 1.6 GB/s |

Plain L4 is in the same class as other per-node proxies. Three things are
not:

- **HTTP mode opens a new upstream connection per request**
  (`src/worker/netd/http.rs`, "no pooling yet"). With mesh mTLS on, that is a
  TLS handshake per request: two extra network round trips on every call,
  even when the client keeps its own connection open. Linkerd and Istio
  pool connections between proxies.
- **The L4 copy is a userspace copy**, though the docs and the plan said
  `splice`. Single-stream throughput is capped near 15 Gbps, and every byte
  costs proxy CPU.
- **No balancing strategy looks at latency.** `round_robin` (the default),
  `least_conn`, `random`, and `local_first` keep sending a slow-but-alive
  backend its full share. Buoyant's slow-pod benchmark found Linkerd's p99
  advantage came from its latency-aware balancer, not from proxy speed.

Smaller costs on the same path: every connection copied the endpoint list
and the in-flight map, `least_conn` saw nothing in HTTP mode because HTTP
mode never counted in-flight requests, and the §7.12 benchmarks did not
exist, so none of this was measured or guarded.

**Found while measuring, and the largest cost of all:** `CounterVec::inc_by`
added its count as a loop of single atomic increments. The datapath reports
each flow's bytes through it when the flow closes
(`legion_net_bytes_total`), so a flow cost one atomic add per byte it
carried. A 64 MiB transfer spun 67 million times on a worker thread at
close. In `net_bench` that held the forwarder to about 0.1 GiB/s at about
50 CPU-seconds per GiB, against 2–2.5 GiB/s and 1 CPU-second for a bare
tokio proxy in the same harness, and it starved other connections on the
same worker while it ran. Every L4 flow through the forwarder, ingress, and
the gated egress proxy paid it.

## 2. Prior art

- **Linkerd** balances with power-of-two-choices over a peak-EWMA latency
  estimate weighted by load, and pools HTTP connections between proxies,
  upgrading HTTP/1 to HTTP/2 on the hop
  ([load balancing](https://linkerd.io/2.15/features/load-balancing/)).
- **Finagle** introduced the peak-EWMA + P2C balancer Linkerd uses.
- **Envoy / Istio sidecars** pool upstream connections per cluster and
  default to round robin, with `LEAST_REQUEST` as P2C.
- **Istio ambient (ztunnel)** carries app connections over pooled HBONE
  (HTTP/2 CONNECT over mTLS) tunnels between nodes.
- **Cilium** keeps plain L4 in the kernel with eBPF socket load balancing.
- **Tensorlake** measured `splice` + kTLS on a sandbox datapath: 2.07 →
  2.50 GB/s ([post](https://www.tensorlake.ai/blog/near-zero-overhead-sandbox-networking)).

We take Linkerd's balancer and connection pooling, and `splice` from the
Tensorlake write-up. We reject eBPF connect-time rewriting for now (§6.6).

## 3. Jobs to be done

**Job author**
- When one replica of a service I call gets slow, I want my calls to go to
  the healthy replicas without my code doing anything, so a bad node does
  not become my p99.
- When my service makes many small HTTP calls to another service, I want the
  platform's proxy not to add a handshake to every one of them, so turning on
  mTLS does not double my latency.

**Cluster operator**
- When a workload streams a lot of data through the node's datapath, I want
  that to cost as little CPU as it can, so the node's capacity goes to tasks.
- When I change the datapath, I want a benchmark that tells me whether I made
  it slower, so performance does not regress quietly.

*Not in scope:* the security admin. Nothing here changes what is allowed.

### User scenarios

1. A three-replica `api` service, one replica on a node with a noisy
   neighbor. Callers using the default strategy shed traffic from it within
   a few requests and come back to it once it recovers.
2. A `web` task calls `users` over HTTP with mesh mTLS. Each call reuses a
   pooled, already-authenticated connection to the `users` node.
3. An operator runs `cargo bench --bench net_bench` before and after a
   datapath change and compares the forwarder rows with the direct rows.

## 4. Goals and non-goals

**Goals**
- Pool upstream connections in HTTP mode, egress and ingress, mesh included.
- `splice(2)` for plain L4 flows on Linux, everywhere the datapath copies.
- A latency-aware strategy, `least_latency`, as the new default.
- Remove per-connection copies of the endpoint list and the in-flight map.
- The §7.12 benchmarks, as Criterion groups.

**Non-goals**
- kTLS (§6.6), HTTP/2 between nodes, multiplexed L4 tunnels (§6.7).
- The `legion netd` process split.
- A cluster-wide load view. Balancing stays per listener.

## 5. What exists today

- `src/worker/forwarder.rs`: `forward_connection` cloned the endpoint list
  and the in-flight map per connection, then `copy_bidirectional`.
- `src/worker/netd/http.rs`: `attempt_request` dialed (and for mesh, ran a
  TLS handshake and wrote a PROXY v2 header) per request, then
  `hyper::client::conn::http1::handshake` per request.
- `src/worker/netd/ingress.rs`, `proxy.rs`, `src/gateway.rs`: each ended an
  L4 flow with `copy_bidirectional`.
- `src/worker/balance.rs`: `Balancer::pick(endpoints, inflight)`, no
  feedback from outcomes.

## 6. Design

### 6.1 `least_latency`, the new default

```nickel
connect = { upstreams = [{ service = "api" }] },                          # least_latency
connect = { upstreams = [{ service = "api", balance = "round_robin" }] }, # the old default
```

Each pick samples two endpoints and takes the one with the lower
`latency × (in-flight + 1)`. The estimate is a peak EWMA: a slow sample
counts in full at once, and estimates decay toward zero with a 10-second time
constant, so an endpoint that was slow is retried once it cools down. A
failure counts as a 1-second sample. An endpoint with no samples starts at
the mean of the others.

What the datapath reports (`Balancer::observe`, a no-op for the other
strategies):

- **L4:** the connect time, then the time from the first request byte to the
  first reply byte. Protocols where the server speaks first produce only the
  connect sample. A dial failure or a mesh handshake failure is a failure.
- **HTTP mode:** the time to the response head. A 5xx or a failed attempt is
  a failure.

HTTP mode now counts its in-flight requests, so both `least_conn` and
`least_latency` see load there.

### 6.2 HTTP-mode connection pooling

Each HTTP-mode listener keeps idle upstream connections, keyed by how they
were dialed: plain to an endpoint, TLS to an external target, mesh mTLS to an
endpoint, or the task itself (ingress). A request checks out an idle
connection or dials a new one, and the connection returns to the pool once
its response body has been read and the upstream kept it alive.

- **Per listener, so per allocation.** A mesh connection names its workload
  in a PROXY v2 header once, at connection start. Pools are never shared
  between allocations, so a pooled connection always carries the right
  identity.
- **Idle limits.** 30 seconds idle, at most 32 idle connections per key.
- **Stale connections.** A pooled connection the upstream already closed is
  skipped at checkout. If the upstream closes it while the request is on the
  way, the request is retried once on a fresh connection when it was never
  written, or when the method is idempotent. Otherwise the request fails with
  a 502, as it would have before.
- **Not pooled.** A `proxy` handler hop (the request goes to the proxy in
  absolute form with per-request attribution headers).

### 6.3 `splice(2)` for plain L4

`netd::splice::bidirectional` replaces `copy_bidirectional` in the
forwarder, ingress, the gated egress proxy, and the gateway. On Linux, when
both ends are plain TCP or Unix sockets, each direction starts as a plain
`read`/`write` copy and switches to `splice(2)` (socket → pipe → socket)
the first time a read fills its 16 KiB buffer, so bulk transfers never
enter the process and small request/reply traffic keeps the cheaper
syscalls. Everything else (TLS, a stream with buffered bytes, macOS) takes
tokio's copy. A probe at first use keeps every flow on the copy on kernels
that refuse splice. Pipe creation failing (out of descriptors) keeps that
direction copying.

### 6.4 The per-byte metric

`inc_by` becomes one atomic add. A regression test adds 2^40 in one call,
which would not finish under the old loop.

### 6.5 Hot-path copies

The endpoint list is stored as `Arc<Vec<Endpoint>>`. The first pick reads it
in place, and only a failover copies it. Picks read the in-flight map under
its lock instead of cloning it.

### 6.6 Deferred: kTLS for mesh hops

kTLS would let mesh hops splice too. It needs a new dependency (or a
hand-rolled handler for TLS 1.3 control records on the receive side),
the kernel `tls` module, and a probe with a fallback. The measured gain
elsewhere is about 20% of bulk throughput, and it does nothing for
connection setup, which pooling addresses. Deferred.

### 6.7 Deferred: multiplexed L4 tunnels, connect-time rewriting

L4 mesh connections still pay a TLS handshake each. ztunnel avoids it with
HBONE, a pooled HTTP/2 tunnel per node pair. That is a wire-format change
for the mesh and gets its own plan. Rewriting plain L4 connections at
`connect()` (Cilium's socket LB) would remove the hop entirely but loses
per-connection accounting and policy; not now.

## 7. Struct and config changes

- `DEFAULT_BALANCE` becomes `least_latency`. No new job-config fields:
  `balance` gains one accepted value.
- `Balancer` gains `observe(&self, addr, Outcome)` with a default no-op.
- `HttpSettings` gains a connection pool.

## 8. Phases

1. **Benchmarks.** `benchmarks/micro/net_bench.rs`: `l4_rr`, `l4_conn_rr`,
   `l4_throughput`, `http_rr`, `slow_endpoint`, `balancer_pick`.
2. **`least_latency` and outcome reporting.** Default flip, HTTP-mode
   in-flight counting.
3. **Splice, the per-byte metric, and hot-path copies.**
4. **HTTP-mode pooling.**

Each ships alone. Phase 1 lands first so every later phase has a before and
after.

## 9. Testing

- Unit: the balancer (sheds a slow endpoint, counts load, breaks ties
  randomly, penalizes failures, decays), splice (both directions,
  half-close, server-first protocols, the copy fallback, the kernel probe on
  Linux), and the pool (reuse, a closed connection is not reused, a stale
  connection retries, per-key isolation).
- Existing forwarder, ingress, proxy, gateway, and HTTP-mode suites run
  unchanged on macOS and on Linux.
- Benchmarks before and after, on Linux.

## 10. Rollout

Upstreams that name no strategy switch from `round_robin` to
`least_latency` on the next worker restart. Jobs that want even spreading
set `balance = "round_robin"`. Nothing else changes on the wire.

## 11. Acceptance

- `cargo bench --bench net_bench` runs and the PR carries before/after.
- `http_rr` through HTTP mode no longer pays a connection per request.
- `l4_throughput` through the forwarder improves on Linux.
- `slow_endpoint` under `least_latency` approaches the fast backends'
  latency.

## 12. Results

`net_bench` on Linux (loopback, an OrbStack VM on a shared and busy host),
base `89229706` against this change, two interleaved A/B rounds:

| | Before | After |
|---|---|---|
| L4 bulk throughput through the forwarder | 80–83 MiB/s | 4.1–4.9 GiB/s |
| Forwarder CPU per GiB, minus direct | 49–51 s | 0.09–0.11 s |
| A bare tokio proxy in the same harness | 2.0 GiB/s, 0.27–0.37 s | same |
| Mean latency, one of three backends 2 ms slower (HTTP / L4) | 1.86 / 1.83–1.91 ms | 62–73 / 98–114 µs |
| `l4_rr`, `l4_conn_rr` | | unchanged within noise |

Most of the throughput gain is the metric fix. `splice` is what puts the
forwarder ahead of a bare userspace proxy, at about a third of its CPU per
byte. On loopback, `http_rr` is too noisy to quote (its own `direct` row
swung between 28 and 106µs across runs), and pooling's real win, a TCP and
mTLS handshake saved per request on a real network, is invisible there.

