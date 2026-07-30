# Judgement calls: heartbeat-scale-plan

Plan: [`../plans/heartbeat-scale-plan.md`](../plans/heartbeat-scale-plan.md)

## 1. Heartbeats stay replicated (batched), rather than becoming leader-local soft state

- **The call:** keep every heartbeat a status-lane replicated write, but
  coalesce concurrent arrivals into one batched op; do *not* move
  liveness to leader-local memory with replicated transitions (the
  Nomad/K8s-lease shape).
- **Alternatives:** (a) leader-local `last_heartbeat` map + replicate
  only alive↔lost transitions; (b) gossip failure detection.
- **Trade-off:** batching cuts consensus rounds by the batch factor
  (bounded by arrival concurrency), not to ~zero the way soft-state
  liveness would; a follower still applies every batch.
- **Why:** (a) changes failover semantics — a new leader starts blind
  and must re-learn liveness, which interacts with the lost-window and
  the startup grace logic — exactly the architecture decision the review
  deferred to AWS-scale evidence (D8). Batching is semantics-preserving
  (same rows, same visibility on followers, same worker-observed
  behavior) and removes the dominant per-heartbeat cost today. The
  benchmark this plan adds is what makes the (a)-vs-batching decision
  answerable later. (Simplicity now, evidence before architecture.)

## 2. Skip-reprojection keyed on the previous status being `active`

- **The call:** the heartbeat apply skips `get_node` + `cache_node`
  (read-model write lock + reindex) when the node's `node_health.status`
  was already `active`, on the argument that a bare timestamp refresh is
  invisible to the scheduling read model.
- **Alternatives:** always reproject (today); diff the full projected
  node for changes.
- **Trade-off:** if some future reader starts consuming
  `last_heartbeat` *from the cached node*, its staleness window grows
  from ~0 to one status transition. The read model's `Node` does carry
  the field; today's consumers of it (liveness scan, API reads) all read
  the DB.
- **Why:** the write-lock acquisition per heartbeat serializes against
  every scheduler read-model access — at 5k heartbeats/s that lock is
  the scheduler's problem, not the heartbeat's. Guarding on a status
  transition keeps every state change fully projected. A code comment at
  the skip site names the invariant new readers must respect.
  (Performance; the invariant is stated where it can be broken.)

## 3. `last_heartbeat_unix` added beside the RFC3339 text column, not replacing it

- **The call:** the liveness scan compares integer epochs; the text
  column stays and keeps being written for API/UI/back-compat.
- **Alternatives:** migrate the column to INTEGER outright; parse text
  but cache parses.
- **Trade-off:** double write (one integer bind per heartbeat) and a
  NULL-fallback parse branch until old rows age out.
- **Why:** replacing the column rewrites every reader (API responses
  format timestamps for humans today) and risks external consumers of
  the API string; a parse cache is more logic for a worse result. The
  double write is one integer bind in an already-executing statement.
  (Simplicity of migration; performance where it is measured — the
  tick.)

## 4. Worker interval configurable, but the lost window deliberately not auto-coupled

- **The call:** `heartbeat_seconds` is worker config (default 2, min 1,
  max 30, warn above 3); the manager's 10s lost window stays fixed, and
  the coupling is documented rather than negotiated per-worker over the
  wire.
- **Alternatives:** (a) keep the cadence hard-coded; (b) workers report
  their interval at registration and the manager scales each node's
  lost window (wire change).
- **Trade-off:** an operator can misconfigure (interval ≥ window) and
  get flapping nodes; the warning and docs are the guardrail, not the
  type system.
- **Why:** at 10k workers the cadence *is* the load knob, and (a) locks
  large fleets into 5k writes/s forever. (b) is the right long-term
  shape but is a proto/wire change with per-node liveness policy —
  deliberately deferred to the D8 architecture decision; noted in the
  plan as deferred. (Ease now, extensibility preserved.)

## 5. `recheck_lost_nodes`: one batched query per tick, NOT the plan's leader-local memory (plan deviation)

- **The call:** the per-tick cost fix is a single query listing every
  pending/in-flight `node_lost_reschedule` eval, diffed in memory
  against the lost set — not the plan's "remember which lost nodes were
  already ensured" leader-local memory.
- **Alternatives:** the plan's suppress-repeats memory; leaving one
  `EXISTS` probe per lost node per tick.
- **Trade-off:** the tick still re-enqueues a reschedule eval for every
  lost node whose previous eval completed — by design (see why) — so a
  large partition keeps generating evals every tick until nodes recover
  or nothing is left to free.
- **Why the deviation:** implementation reading showed the per-tick
  re-enqueue is load-bearing, not waste — a completed reschedule pass
  must be re-driven so stateful allocations whose `onLost.replaceAfter`
  threshold elapses *later* get replaced without operator action
  (`enqueue_lost_node_reschedule`'s own doc says so). The plan's memory
  would have silently broken `replaceAfter`. The batched query keeps the
  semantics bit-for-bit and still removes the O(lost-nodes) query storm
  — a 2,000-node partition costs one query per tick instead of 2,000.
  (Correctness over the plan's letter; the plan's *goal* — cheap ticks —
  is met.)

## 6. Batched heartbeats keep per-id NotFound verdicts

- **The call:** a heartbeat batch returns each caller its own verdict:
  registered nodes succeed, an unregistered ("ghost") node's heartbeat
  errors typed `NotFound` exactly as the single op always did.
- **Alternatives:** one shared batch verdict (all-succeed unless the
  batch fails); treating ghosts as silent no-ops.
- **Trade-off:** the batched apply tracks a ghost set and the batch
  runner maps results per id — a little machinery a shared verdict
  would not need.
- **Why:** a worker whose node was removed must not believe it is
  healthy — its self-fence clock advances on heartbeat *success*, and a
  ghost that keeps "succeeding" would keep orphaned stateful tasks alive
  past their fence window. The typed error also keeps the RPC surface's
  NotFound mapping intact. (Correctness of the fencing contract over
  batching simplicity.)

## 7. Heartbeat coalescing has no config knob

- **The call:** heartbeat group commit is unconditional; only the
  plan-apply coalescer got an escape hatch.
- **Alternatives:** a `heartbeat_coalescing: false` setting for
  symmetry.
- **Trade-off:** an operator cannot restore strictly-serial heartbeat
  commits without downgrading.
- **Why:** a batch of one *is* the old path (same replicated op shape),
  batching preserves per-caller semantics including error verdicts, and
  heartbeats have no equivalent of the plan path's per-plan isolation
  subtleties that might justify a kill switch. Every knob is surface to
  document and test. (Simplicity; the escape hatch would guard against
  nothing nameable.)

## 8. The reprojection skip consults the cached view, not just the database

- **The call:** a heartbeat skips the read-model reprojection only when
  the node is active in the DB *and* available in the cached read model;
  a mismatch always reprojects.
- **Alternatives:** DB-status check alone (the plan's letter).
- **Trade-off:** one read-model read-lock acquisition per heartbeat
  batch (cheap; shared, not exclusive).
- **Why:** a concurrent mark-lost can publish its cache update after the
  heartbeat's DB write; with a DB-only check the next heartbeat sees
  "already active" and skips forever while the cache still says lost —
  a permanently wrong scheduler view. Consulting the cache converges it
  on the next beat, which is exactly the guarantee the old
  unconditional reprojection gave. (Correctness under the race the old
  code healed by accident.)
