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

## 5. `recheck_lost_nodes` gets leader-local memory, not a schema flag

- **The call:** remember "reschedule already ensured" for lost nodes in
  leader memory (cleared on transition or leadership change), instead of
  persisting a `reschedule_enqueued` marker on `node_health`.
- **Alternatives:** a persisted flag column; leaving the per-tick
  re-query in place.
- **Trade-off:** a fresh leader re-runs the existence check once per
  lost node (the check is the safety net; the memory only suppresses
  *repeats*), costing one redundant query per lost node per leadership
  change.
- **Why:** the check is idempotent and cheap once — what hurts is
  running it for every lost node every 2 seconds forever. Leader-local
  memory removes the steady-state cost with no migration and no new
  replicated state to keep consistent; the persisted flag would need
  invalidation rules for exactly the transitions the memory handles
  trivially. (Simplicity; the failure mode degrades to today's
  behavior.)
