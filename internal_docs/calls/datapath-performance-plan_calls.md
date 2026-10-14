# Judgement Calls: Datapath Performance

For [`plans/datapath-performance-plan.md`](../plans/datapath-performance-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. `least_latency` is the new default strategy

**Call.** An upstream that names no `balance` now gets `least_latency`
(power of two choices over a peak-EWMA latency estimate, weighted by
in-flight load) instead of `round_robin`.

```nickel
connect = { upstreams = [{ service = "api" }] },                          # least_latency
connect = { upstreams = [{ service = "api", balance = "round_robin" }] }, # the old default
```

**Alternatives.** (a) Ship `least_latency` as an opt-in strategy and keep
`round_robin` as the default: zero surprise, and what a cautious release
would do. (b) Envoy's default, round robin, with P2C least-request as an
option.

**Trade-off.** The spread is no longer even or predictable. A demo that
expects strict alternation has to ask for `round_robin` (the connect demo
now does). The strategy favors endpoints on the caller's node, because they
connect faster. On L4 it learns only from connect time and time to first
reply, so a pool of long-lived database connections is balanced by connect
time and load rather than by query latency. Revisit if operators report
skew toward node-local backends that load alone does not correct.

**Why (performance, and the right default).** The slow-but-alive backend is
the common way a fleet's p99 goes bad, and every strategy we had sent it a
full share. `slow_endpoint` in `net_bench` shows the difference directly.
Linkerd defaults to the same balancer for the same reason. AGENTS.md is
explicit that a new cluster should get the right default, not the
backward-compatible one.

## 2. Pools are per listener, so per allocation, never node-wide

**Call.** Each HTTP-mode listener keeps its own pool of idle upstream
connections. Two allocations on one node calling the same service do not
share connections.

**Alternatives.** A node-wide pool keyed by destination, which reuses more
when many small tasks call the same service from one node.

**Trade-off.** Less reuse on dense nodes: N allocations calling `users` hold
up to N sets of idle connections and each pays its own first handshake.

**Why (correctness over reuse).** A mesh connection names its workload in a
PROXY v2 header once, when it opens, and the server attributes every
request on that connection to it. A shared pool would attribute one
workload's requests to another, which is an identity bug, not a performance
trade. Per-listener pools make that impossible by construction.

## 3. kTLS is deferred, though it was on the list

**Call.** This change does not implement kTLS for mesh hops, which was the
fourth item on the original list.

**Alternatives.** (a) The `ktls` crate plus rustls `secret_extraction`.
(b) Hand-rolled: install keys with `setsockopt(TLS_TX/TLS_RX)` and handle
TLS 1.3 control records (session tickets, key updates) through `recvmsg`
control messages ourselves.

**Trade-off.** Mesh hops still copy and encrypt in user space, so bulk
throughput over mesh mTLS stays where it was.

**Why (simplicity, and the evidence).** (a) adds a dependency, which needs
agreement first. (b) puts a TLS record-layer state machine in the datapath.
The measured gain elsewhere is about 20% of bulk throughput (Tensorlake,
2.07 → 2.50 GB/s), it does nothing for connection setup, and connection
setup is what pooling fixes. It is named as deferred in the plan and in
TODO.md.

## 4. Stale pooled connections retry once, only when safe

**Call.** When a pooled connection turns out to be closed, the request is
retried once on a fresh connection if hyper reports it was never written,
or if its method is idempotent. A POST that was written and then lost its
connection fails with a 502.

**Alternatives.** (a) `hyper_util::client::legacy::Client`, whose pool has
its own retry rules. (b) Never retry; let the job's `retries` policy decide.

**Trade-off.** A non-idempotent request can still fail in the narrow window
where the upstream closes an idle connection just as it is reused. The
30-second idle limit (shorter than most servers' keep-alive) and the
liveness check at checkout keep that window small.

**Why (simplicity, correctness).** (a) dials through a `tower` connector
keyed by URI, which cannot express our dial kinds (mesh with a PROXY header,
a task inside a namespace, a sandbox over vsock) without encoding them into
fake URIs. (b) would turn a pool artifact the task never asked for into
user-visible errors. Retrying an unsent request is always safe, and retrying
an idempotent one is what HTTP allows.

## 5. `splice(2)` by hand with `libc`, not a crate

**Call.** The splice loop is about 150 lines over `libc::splice` and tokio's
`ready`/`try_io`, behind a `SpliceStream` trait that says whether a stream
is a raw socket.

**Alternatives.** `tokio-splice` or `nix`, which wrap the same syscall.

**Trade-off.** We own a small amount of `unsafe` code and its edge cases
(half-close, would-block bookkeeping).

**Why (minimal dependencies).** `libc` is already a dependency, and the
crates add little over it. The loop drains the pipe after every fill, so a
would-block can only come from a socket, which keeps the readiness
bookkeeping simple enough to read.

## 6. Splice only bulk directions, and only when nothing has to see the bytes

**Call.** A flow is eligible only when both ends are plain TCP or Unix
sockets. TLS, a stream with a buffered prefix, and every non-Linux platform
take tokio's copy. On an eligible flow, each direction starts as a plain
`read`/`write` copy through a 16 KiB buffer and switches to `splice` the
first time a read fills that buffer (HAProxy's "splice-auto"). The pipe is
created at that switch. A probe at first use keeps every flow on the copy
if the kernel refuses `splice`, and a direction whose pipe cannot be created
(out of descriptors) keeps copying.

**This reverses the first version.** That version spliced every eligible
flow from its first byte with two pipes per flow. `net_bench` showed it
costing about 1.6µs more per round trip on `l4_rr` (100-byte messages, both
A/B pairs, Linux): for a few hundred bytes, two `splice` calls and the pipe
bookkeeping cost more than one `read` and one `write`, and every short-lived
connection paid two `pipe2` calls and four closes for nothing.

**Alternatives.** Splice everything (the first version). Splice the
plaintext side of TLS flows too. Pick by port or protocol.

**Trade-off.** A bulk direction pays one copied buffer before it switches.
Ingress with a PROXY header, TLS termination, and mesh hops gain nothing.
Past the per-user pipe-page soft limit
(`/proc/sys/fs/pipe-user-pages-soft`), Linux creates small pipes, so a node
with tens of thousands of bulk flows at once splices in smaller steps. Idle
and chatty connections no longer hold pipes at all.

**Why (performance, without a regression).** Most service traffic is small
requests and replies, and bulk transfers are where copying costs. The
heuristic gives each the cheaper path without a knob. The paths that see
bytes are the ones that must, and they stay byte-for-byte what they were.

## 7. L4 latency is connect time plus time to first reply

**Call.** Without parsing anything, the L4 path reports the connect time and
then the time from the first byte the client sends to the first byte the
endpoint returns. A flow where the server speaks first reports only the
connect time.

**Alternatives.** Connect time only. Or connection duration, which measures
how long a client kept a connection, not how fast the server answered.

**Trade-off.** One sample per connection. Long-lived connections learn
slowly (see entry 1).

**Why (performance).** Time to first reply is the closest thing to request
latency an L4 proxy can see, and it costs one atomic and one clock read per
flow.

## 8. Benchmarks are Criterion on loopback, not a CI gate

**Call.** `benchmarks/micro/net_bench.rs` runs the real `UpstreamForwarder`
against loopback backends. It is run by hand and its numbers go in PRs, like
`scheduler_bench`. It does not fail CI on regression.

**Alternatives.** The plan's §7.12 wording: benchmarks that fail on
regression.

**Trade-off.** A regression is caught when someone runs the benchmark, not
automatically.

**Why (signal over noise).** Shared CI runners vary by more than the
regressions we care about (a few microseconds per hop), so a hard gate would
flake. Each group has a `direct` row next to the forwarder rows, so a run on
any machine is self-comparing.

## 9. Pool limits: 30 seconds idle, 32 idle per destination

**Call.** Idle connections leave the pool after 30 seconds, and at most 32
are kept per destination per listener.

**Alternatives.** hyper-util's 90-second default; Envoy's one-hour idle
timeout.

**Trade-off.** A listener that goes quiet for 30 seconds pays a new
handshake on its next request.

**Why (correctness).** Most HTTP servers close idle keep-alive connections
somewhere between 5 and 75 seconds. Staying under the common values keeps
the stale-connection window of entry 4 small. 32 covers bursts without
holding a large set of descriptors per endpoint.

## 10. Estimator constants follow Finagle and Linkerd

**Call.** A 10-second decay, a 1-second failure penalty, and new endpoints
starting at the mean of the known estimates.

**Alternatives.** Expose the constants as `balance` options.

**Trade-off.** No tuning knob.

**Why (simplicity).** These are the values two production balancers settled
on, and an operator who needs different behavior has four other strategies.
A knob can come later if someone needs one.
