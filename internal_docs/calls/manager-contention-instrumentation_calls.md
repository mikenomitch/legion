# Judgement Calls — Manager Contention Instrumentation (2026-08-02)

Instrumentation added after the first real AWS rig found a manager
throughput ceiling (~80 heartbeat RPCs/sec against a 3-voter `lanes` control
plane) that the existing metrics could not explain: `Heartbeat` p99 56ms,
`AppendEntries` p99 3ms, `HealthCheck` p99 30ms, CPU 19-23%. The open item
is in [TODO.md](../TODO.md); the design answer for cadence is
[heartbeat-cadence-at-scale-plan.md](../plans/heartbeat-cadence-at-scale-plan.md).
No plan owns this batch — it extends an existing documented surface
(`docs/metrics.md`) rather than adding one — so the calls live here, named
after the work. **Entries are ranked by project impact; the number is the
rank.**

## 1. Instrumentation ships alone; the blocking heartbeat path is not touched

**The call.** This change adds metrics only. `Manager::heartbeat` still calls
`store.update_node_heartbeat` synchronously from an async handler, the
coalescer still polls on a 1ms `recv_timeout`, and `HEARTBEAT_BATCH_MAX`
still forms batches only from requests that arrive mid-commit. Nothing about
the write path moves.

**The alternative.** Fix and measure in one change — wrap the store call in
`spawn_blocking`, then confirm the ceiling lifted. The evidence pointing at
the blocking call is strong (idle CPU, a do-nothing `HealthCheck` at 30ms,
fast `AppendEntries`), and shipping both would have closed the issue in one
pass.

**The trade-off.** The ceiling stays up until a second change lands, and the
new families are validated only by unit tests plus a local run rather than
by the AWS rig that motivated them.

**Why.** A fix and its instrumentation shipped together cannot be told apart
afterwards: if throughput improves, "the blocking call was the problem" and
"something else in the same diff was the problem" produce the same graph.
The reproduction attempt reached 498/s locally, so the mechanism is *not*
confirmed — local NVMe fsync is far faster than gp3 EBS, and the hypothesis
rests on a difference the repro could not exhibit. Instrumentation first
turns the next AWS run into a measurement instead of a second guess. It also
means the metrics get designed against the *questions*, not against a fix
that already assumes the answer.

## 2. Runtime saturation is measured with a spawn-delay probe, not per-request arrival timestamps

**The call.** A task spawned once a second records how long it waited for
its first poll (`legion_runtime_spawn_delay_seconds`), alongside a
worker-thread gauge and a per-method in-flight gauge.

**The alternative.** Timestamp each request at arrival and record
arrival-to-handler-start per RPC — the direct measurement, per method, with
a distribution rather than a 1 Hz sample.

**The trade-off.** The probe is a sample, not a census: it cannot attribute
delay to a method, and a 1 Hz sample can miss a short stall entirely. It
also measures the runtime rather than the request, so a genuinely slow
handler and a starved one look the same until in-flight is read next to it.

**Why.** Under tonic the honest arrival point is inside the tower layer, and
the layer's own `call` already runs on a runtime worker — a timestamp taken
there measures the tail of the stall, not its head, so the direct
measurement is less direct than it appears. Meanwhile the condition being
hunted is global by nature: when workers are occupied, *every* handler slows
down, which is exactly why the 30ms `HealthCheck` p99 was the most
informative number in the original report. One spawn a second costs nothing
and names that condition unambiguously — tens of microseconds healthy,
milliseconds saturated. Simplicity won over resolution.

## 3. The coalescer's wait is one histogram split by role, not two families

**The call.** `legion_store_coalesce_wait_seconds{path, role}` records a
submitter's wait once, under the role that ended it: `leader` (won the
commit lock, its own commit not yet started) or `follower` (another
submitter's batch returned its result).

**The alternative.** Two families — a pure lock-wait histogram and a
served-by-another-batch histogram — so neither has to be read with a label
selector, and each can carry its own help text.

**The trade-off.** The two roles measure genuinely different intervals: a
follower's wait *includes* the commit it waited on, a leader's excludes its
own. Anyone summing across the label without reading the help text gets a
number that means nothing.

**Why.** Both intervals answer the same question — "was this submitter
waiting or working?" — and the ratio between the two roles is itself the
diagnosis: near-100% `leader` with batch size 1 means batching never
engages, while heavy `follower` means it does and the commits are the cost.
Splitting them into separate families hides that comparison behind a join.
The naming makes the asymmetry explicit in the help string and in
docs/metrics.md.

## 4. The coalescer's `path` label is a constructor argument, not a type parameter or a call-site string

**The call.** `Coalescer::new("heartbeat", HEARTBEAT_BATCH_MAX)` — the
generic primitive carries a `&'static str` for its metric label:

```rust
heartbeat_coalescer: Arc::new(Coalescer::new("heartbeat", HEARTBEAT_BATCH_MAX)),
plan_coalescer: Arc::new(Coalescer::new("plan_apply", plan_batch_max)),
```

**The alternative.** Keep `coalesce.rs` free of telemetry entirely and have
each caller time its own `submit` — the primitive stays a pure data
structure with no dependency on `crate::manager::telemetry`.

**The trade-off.** A storage-layer primitive now reaches into the manager's
metric registry, and every future coalesced path must pick a label (and keep
it low-cardinality) to get instrumented.

**Why.** The interesting quantities live *inside* `submit` and are not
observable from outside it: batch size, which submitter led, and the
1ms poll rounds are all invisible to a caller that only sees total elapsed
time. Instrumenting at the call site could only ever re-derive total
latency, which the RPC handler's histogram already provides — and that
histogram is precisely what failed to explain the ceiling. The dependency
direction is already established (`storage/raft/driver.rs` records the
`legion_raft_*` families the same way).

## 5. Pipeline queue depth is a paired-increment gauge, accepting drift on an impossible path

**The call.** `legion_raft_pipeline_depth{lane}` is incremented by the
submitter and decremented by the pipeline thread when it picks the job up
(plus once more if the send fails because the thread is gone).

**The alternative.** Derive depth from the two histogram counts
(submitted minus started) at query time, so no gauge can ever drift, or ask
the `std::sync::mpsc` channel for its length — which it does not expose.

**The trade-off.** A panic between the increment and the receive would leak
the gauge upward permanently. `pipeline_loop` catches panics inside job
execution, so the window is the queue itself, but the gauge is not
self-healing the way a periodically-refreshed one is.

**Why.** Depth is the metric an operator reads at a glance during an
incident — "is this lane backed up right now?" — and a derived
counter-difference is neither glanceable nor available on a dashboard
without a recording rule. Both decrement sites are on the only two paths a
job can take, and a leaked gauge would be visible as a depth that never
returns to zero, which is itself a legible failure. The
`legion_grpc_in_flight` gauge takes the same shape but has a real
cancellation path, so it uses a `Drop` guard rather than a bare decrement.

## 6. `legion_raft_pipeline_*` claims the reserved `legion_raft_` prefix

**The call.** The per-lane queue in front of the replication pipeline is
named `legion_raft_pipeline_wait_seconds` / `legion_raft_pipeline_depth`,
under a prefix docs/metrics.md reserves for the raft engine.

**The alternative.** Name it after the store (`legion_store_replication_*`)
and leave `legion_raft_*` strictly for engine-internal series, as the
reservation note asks.

**The trade-off.** The reservation note now needs an explicit exception, and
these two families are recorded on every node rather than leaders only —
unlike every other `legion_raft_*` latency family, which is a leaders-only
signal. That asymmetry is documented but it is a wrinkle in an otherwise
uniform prefix.

**Why.** The queue exists *because* of the raft engine's one-pipeline-thread-
per-lane design (JC-3), carries the same `lane` label, and is read directly
against `legion_raft_capture_seconds` and
`legion_raft_propose_apply_seconds` — the three together decompose one
replicated write end to end. Splitting the first term of that decomposition
into another prefix would make the most useful comparison in the whole set
the least discoverable. The reservation exists to stop unrelated subsystems
squatting the prefix; this is the raft write path.
