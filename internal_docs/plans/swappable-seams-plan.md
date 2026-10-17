# Plan: Swappable seams in the datapath, object storage, and the cluster CA

> **Judgement calls:** [`calls/swappable-seams-plan_calls.md`](../calls/swappable-seams-plan_calls.md)

**Status: SHIPPED** (2026-10-14, one PR). The cluster CA half has its own
plan, [cluster-ca-provider-plan.md](cluster-ca-provider-plan.md), because it
is a new config surface and a security boundary; this plan covers the rest
and points there. Deliberately deferred, each an open item in
[TODO.md](../TODO.md): the `legion netd` process split itself (the boundary
type here is its prerequisite, not the split), a wire encoding for
`NetdInput`, native GCS and Azure object stores, a `Vmm` trait for the
sandbox runtime, and a watchable service catalog for outside load balancers.

---

## 1. Problem statement

An audit of the networking stack and the other recent subsystems asked one
question: if we had to swap an implementation tomorrow, what would it cost?
Most answers were fine — balancers are a registry, handlers extend out of
process, the plugin categories have real traits. Five were not:

1. **No type says how an allocation is attached to the datapath.** Host
   loopback, a gated namespace, a gated sandbox over vsock, or a TAP guest
   was re-derived from `(network.mode, task_runtime == "sandbox", sandbox
   config JSON)` at about 40 sites in 15 files. Specs encoded the choice as
   mutually exclusive `Option<PathBuf>` fields, and "fail closed" was
   inferred from which one was set (`gated = netns.is_some() ||
   vsock.is_some()`). Any new capture mode (a bridge, TPROXY/eBPF, TUN) had
   to find every one of those sites.
2. **The worker ↔ datapath boundary was a convention.** `netd/mod.rs` said
   the interface was "policy in, telemetry out, capture setup and
   teardown", but `daemon.rs` held ~750 lines of spec building and called
   ~20 setters on a 25-field `ForwarderContext`. The `legion netd` process
   split (service-mesh calls 17) would have been a design job, not an
   extraction.
3. **Object storage had no shared layer.** Three S3 client builders
   resolved credentials three ways, `file://` was written five times, and
   the URL scheme was matched in six places. Two bugs came from that split
   already (`8f1078a`, `bb86470`).
4. **Two protocol parsers were hand-rolled where a library fits.** The
   per-allocation resolver parsed DNS by hand (UDP-only forwarding, so a
   truncated answer stayed truncated, and a silent `1.1.1.1` fallback when
   `resolv.conf` named no nameserver). The egress proxy parsed HTTP heads
   by hand and judged only the first request on a connection; anything
   pipelined behind it was spliced through unchecked.
5. **The cluster CA's issuer was hard-wired** — covered by its own plan.

Two bugs surfaced on the way and are fixed here: VIP blocks were assigned by
the *manager's* OS instead of the node's, and the worker dropped the
endpoint feed's `mesh_tls` flag, so connect upstreams dialed mesh-TLS
endpoints in plaintext and were refused at the handshake.

## 2. Prior art

- **Kubernetes / CNI.** The kubelet hands a runtime a netns path and calls a
  CNI plugin with it; the plugin owns the wiring. Our `Attachment::Netns` is
  that contract on our side, which is why a later CNI mode is one variant.
- **Istio ambient / ztunnel.** A node agent with a narrow control
  interface (xDS in, telemetry out) separate from the workload agent. The
  `Netd` boundary is the in-process version of that split.
- **Nomad.** `client/allocrunner/network_hook` computes one network
  "mode" per allocation group and every later hook reads it — the shape we
  copy with `Attachment::of`.
- **`object_store` (Arrow/DataFusion)** is the de-facto Rust object-store
  abstraction; the gap was that we did not route every consumer through it.
- **hickory-dns** and **hyper** are the Rust ecosystem's DNS and HTTP
  libraries; CoreDNS and Envoy are the comparators for what a node resolver
  and a forward proxy should do with truncation and pipelining.

## 3. Jobs to be done, by persona

**Legion contributor**
- When I add a new way for a workload to reach the network, I want to add it
  in one place, so I do not miss a listener and ship a hole.
- When I split the datapath into its own process, I want the messages
  already defined, so the work is moving code, not designing a protocol.
- When I add a cloud's object store, I want one module to teach, so every
  feature that stores objects gets it at once.

**Cluster operator**
- When a node has no DNS resolver configured, I want lookups to fail loudly,
  so traffic never goes to a resolver I did not choose.
- When my manager runs on a Mac and my workers on Linux, I want service names
  to work on the workers, so my dev cluster behaves like production.

**Security / platform admin**
- When I write an egress rule on a path, I want every request checked, so a
  client cannot slip a forbidden request behind an allowed one.

**Job author**
- When my service calls another over the mesh, I want the call to succeed, so
  turning on mTLS does not break the services that call mine.

*Not in scope:* the autonomous-agent persona — nothing here changes an agent
surface.

## 4. Goals and non-goals

**Goals.** One attachment type; one boundary type between worker and
datapath; one listener and one dialer; one object-store layer with one
credential path per consumer behavior; library parsers for DNS and HTTP; the
two bugs fixed. No user-visible change except the DNS fallback, the proxy's
per-request verdicts, and the two fixes.

**Non-goals.** A datapath plugin ABI or CNI support (service-mesh calls 5
still stands — this makes it cheaper later, not now). The `legion netd`
process split. New object-store backends. A `Vmm` trait. Splitting
`ForwarderContext` (it is now internal to `netd`).

## 5. Design

### 5.1 `Attachment` (`src/worker/netd/attach.rs`)

```rust
pub enum Attachment {
    Host,                            // node loopback; cooperative policy
    Netns { path: PathBuf },         // gated exec/container; fails closed
    GuestVsock { scratch: PathBuf }, // gated sandbox; VMM vsock host sockets
    GuestTap { scratch: PathBuf },   // TAP-wired sandbox; bridge address
}
```

`Attachment::of(allocation, network, alloc_root)` decides it once.
`is_gated()` replaces the transport inference; `listen(ip, port)` returns
where any listener binds; `task_dial()` returns how ingress reaches the task.
Every spec (`UpstreamSpec`, `DnsSpec`, `ProxySpec`) carries it.

### 5.2 One listener, one dialer (`src/worker/netd/listen.rs`)

`ListenAddr::{Tcp { netns, addr }, Unix(path)}` binds; `spawn_accept_loop`
runs every listener's accept loop (backoff on errors, a task per
connection); `Stream` is the accepted connection; `dial_tcp` is every
outbound TCP dial. The four copies of accept-loop plumbing and five
hard-coded `TcpStream::connect` calls are gone.

### 5.3 The worker ↔ datapath boundary (`src/worker/netd/datapath.rs`)

```rust
pub enum NetdInput {
    Desired(DesiredNetwork),   // desired set + endpoint feed + assignments
    NodeId(String),
    PolicyBody { hash, policy },
    MeshIdentity(MeshIdentity),
    IngressCertificates(Vec<IngressCertificate>),
    UpstreamCredentials(Vec<UpstreamCredentialValue>),
    RefreshNodeAddresses,
    KernelGuard(Vec<GuardedAllocation>),
}
pub struct NetdWants { policy_bodies, ingress_certificates, upstream_credentials }
pub struct NetdReport { denials, edges, statuses }
```

`Netd::send` takes an input, `wants()` says what to fetch from the manager,
`report()` drains what the next heartbeat carries and `redrive()` puts it
back after a failed one. The worker's heartbeat now builds a
`DesiredNetwork`, sends it, fetches what `wants()` names, and sends the
answers. Spec building, status, namespace capture, persistence and restore
moved into `netd` (`specs.rs`, `datapath.rs`). The persisted
`netd_state.json` is `DesiredNetwork` plus policy bodies; its on-disk shape
is unchanged.

### 5.4 Object storage (`src/storage/objects/`)

One URL parser, one `open(url, credential, options)` returning
`Arc<dyn ObjectStore>`, one credential resolver; content publish, the S3
artifact fetcher, session manifests, the manager archive, and streaming
persistence all go through it. `OpenOptions` names what legitimately differs
between consumers (conditional puts, the region default, whether the
process's other `AWS_*` settings apply, and whether an ambiguous ambient
environment falls through or is refused). The behavior each consumer kept,
and the three bugs the unification fixed, are calls entries 3 and 6–8.

### 5.5 Protocols

- **DNS** on `hickory-proto` (parse/build only). A TCP client gets a
  truncated UDP answer refetched over TCP. A node whose `resolv.conf` names
  no nameserver answers `SERVFAIL` and warns once at start.
- **Egress proxy HTTP** on hyper's server: `CONNECT` via upgrades (the
  `200 Connection Established` reason phrase kept), absolute-URI requests
  judged one by one, kept alive or pipelined, each sent upstream on its own
  connection in origin form.

### 5.6 Bugs

- Placement decides VIP blocks with `node_supports_vip_blocks(node os)`
  (`src/structs/network.rs`), reading the node's `os` fingerprint.
- `specs::dialable_endpoints` carries `mesh_tls` from the feed.

## 6. Phases

Shipped together; each was independently testable and committed on its own:

1. Attachment, listener/dialer, and the `Netd` boundary (plus the
   `mesh_tls` fix, found while moving `dialable_endpoints`).
2. DNS on hickory-proto, TCP refetch, no public fallback.
3. Egress proxy on hyper.
4. VIP placement by node OS.
5. Object-storage layer.
6. Cluster CA providers ([own plan](cluster-ca-provider-plan.md)).

## 7. Testing

Unit tests moved with the code they cover (`netd::specs`, `netd::datapath`)
and new ones pin each behavior: `Attachment::of` for every runtime and mode;
one accept loop serving TCP and Unix sockets; a desired set starting and
feeding listeners; a mesh endpoint wanting the mesh; `wants()` following the
desired set; `redrive()`; restore from disk; DNS `SERVFAIL` with no resolver
and TCP refetch of a truncated answer; a pipelined allowed-then-denied pair
getting 200 then 403; VIP blocks by node OS. The full suite runs in CI.

## 8. Rollout

No migration. The persisted datapath state keeps its shape. Visible changes,
all in the PR description and the public docs: `SERVFAIL` instead of a
`1.1.1.1` fallback, per-request verdicts and keep-alive in the egress proxy,
VIP blocks by node OS, mesh upstreams that now work.

## 9. Acceptance

- No site outside `netd` derives a listener's binding from the mode string or
  the runtime name.
- Past constructing it, `daemon.rs` touches the datapath only through `Netd`.
- No consumer outside `src/storage/objects/` builds an S3 client.
- The tests above pass.
