# Judgement Calls — autoscaler-plan

Decisions made while designing
[`plans/autoscaler-plan.md`](../plans/autoscaler-plan.md). **Ordered by project
impact, highest first; the entry number is its rank.** These are design-time
calls; implementation will append (and re-rank) its own.

## 1. Desired count is a separate, owned record — not a jobspec resubmit

**Call.** `legion job scale` writes a count *override* keyed by the
version-stable `(job, group, task)` tuple into a new `task_scale` table.
Effective desired count = override, else spec count. Scaling never creates a
job config version and never starts a rollout.

```
$ legion job scale web --count 6 --reason "launch traffic"
web/default/web: 4 -> 6 (spec count 2; override active)
```

**Alternatives.** (a) Nomad's answer: `Scale` internally registers a new job
version with the count edited — one write path, version history is complete.
(b) Make count-only diffs a special case inside `is_same_as` so resubmits with
only count changes skip the version bump — no new table, no new RPC.

**Trade-off.** A second source of truth for desired count. Every reader of
`count.num` (`tasks_for_plan_from_store`, `slot_still_desired_tx`, rollout
`build_group_plans`, staged-slot math) must consult the override, and the raw
SQL reader (`slot_still_desired_tx`) is easy to miss in review. Version
history no longer tells the whole capacity story; the override row + scaling
events do. Alternative (b) was rejected because it still cancels in-flight
rollouts on resubmit and still gives the autoscaler no way to act without
holding the full jobspec.

**Why (simplicity + performance).** Nomad's model floods version history under
active autoscaling and resets counts on resubmit
([nomad#9839](https://github.com/hashicorp/nomad/issues/9839)); K8s proved the
subresource split is the design that ages well. "Spec changes make versions,
`job scale` doesn't" is a one-sentence rule an operator can hold at 3am, and a
scale action becomes one small Scheduler-lane write instead of a full spec
re-register.

## 2. The autoscaler is a manager subsystem first, a job second

**Call.** Day one, the evaluator is an in-core loop beside
`start_node_status_checker`, leader-gated on `is_raft_leader(Lane::Scheduler)`,
default-enabled, no-op without policies. The identical code ships as
`legion autoscaler agent` for running as a normal job (control-plane identity,
KV-lock leadership), and this "escape-to-job" shape is documented as the
template for every later ecosystem app.

**Alternatives.** (a) Nomad's: a separate agent only — one deployment story,
core stays smaller. (b) The user-floated variant: built-in apps run as *jobs
on manager nodes* from day one, via manager-as-worker plus an admin-gated
placement mechanism.

**Trade-off.** Autoscaler code lives in the manager binary and its bugs are
manager bugs (mitigated: the loop only calls the same `ScaleTask` path as the
CLI, so a wedged evaluator can't corrupt state, only stop scaling). The
job-on-managers idea also loses something real by being deferred: there is no
uniform "system placement" story yet, so in-core apps and job apps are two
mechanisms rather than one.

**Why (simplicity/ease).** A day-one cluster gets autoscaling with zero extra
processes, no HA sidecar, and policies in cluster state — dissolving the three
loudest Nomad-daemon complaints. Alternative (b) front-loads a bootstrapping
problem (the thing that schedules jobs depending on a scheduled job) and a
placement/ACL mechanism this feature doesn't need; `--run-worker` already
exists for anyone who wants managers schedulable, and a system-jobs plan can
absorb built-ins later without this plan blocking on it.

## 3. Policies are declarative data; user-submitted *logic* waits for WASM

**Call.** v1 policies are config: built-in sources (`legion`, `prometheus`)
and strategies (`target-value`, `per-replica`) behind internal traits.
Operator-installed dylib plugins come later via the standard checklist;
registry-shared strategies are WASM-only, per the sandboxing design the mesh
plan already did (§D3). Policy *reuse* needs no machinery at all — jsonnet
imports are the sharing mechanism until the registry exists.

**Alternatives.** (a) Ship dylib strategy plugins in v1 (the rollout-strategy
kind took days, not weeks). (b) Accept user-submitted scripts/expressions in
the policy itself (Lua/CEL-style) executed by the manager — maximum
flexibility immediately, and closest to "users submit target logic to the
scheduler."

**Trade-off.** Users with a genuinely novel scaling function can't express it
in v1 (the pressure valve: `per-replica` over a Prometheus query lets an
external system compute counts — Nomad's `pass-through` niche). Accepted: the
extensible-strategy audience is small until the registry exists to share them.

**Why (extensibility *and* the security boundary).** Arbitrary policy logic
executing in the manager is exactly the "full cluster compromise" argument the
auth plan used to refuse auth-method dylibs. Keeping the strategy trait pure
(`samples + count -> count`, no I/O) keeps it WASM-compilable, so the
deliberate v1 closure of this seam is what makes the registry story safe
later.

## 4. Resubmit ownership: the policy owns count; otherwise `base_count` arbitrates

**Call.** With a `scaling` block, spec `count` is initial-only and resubmits
never reset the live count. Without one, a manual override records the spec
count it was laid over (`base_count`); a resubmit whose count still equals
`base_count` keeps the override, one that differs clears it and wins — with
the outcome stated in the submit reply either way.

**Alternatives.** (a) Spec always wins (Nomad): simplest, and silently reverts
scaling on every deploy. (b) Override always wins until `--reset`: never
reverts, but makes editing `count` in a spec a no-op — the confusion K8s
answers with "delete `replicas` from your manifest," which we can't copy
because our count field has a job (initial value, keyed/slot validation).

**Trade-off.** A third rule to document, and a resubmit's effect on count now
depends on state not visible in the diff (mitigated by the reply message and
`job scale-status`). The matrix (spec count same/changed/absent × override ×
policy) needs exhaustive tests.

**Why (simplicity/ease).** Both intuitions users actually have — "my scale
survives deploys" and "editing the spec does what it says" — hold
simultaneously. It is server-side apply's field ownership reduced to one
stored integer and one comparison.

## 5. `task_scale` is CONTROL-lane, not scheduler-lane (implementation deviation from plan §6.1)

**Call.** (Implementation, 2026-07-30.) The `task_scale` table and its four
ops (`set_task_scale_override`, `set_task_scale_pending`,
`clear_task_scale`, `promote_pending_task_scales`) ride the **Control**
lane, where the plan said "scale writes ride the Scheduler lane". The
reconciling `job_scale` eval enqueue stays a separate sequential
scheduler-lane op (the stop_job shape), and `retire_surplus_allocations`
(touching `allocation`) is scheduler-lane.

**The alternative.** The plan's lane: scheduler-owned, letting the override
write and the eval enqueue share one capture.

**The trade-off.** A `job scale` is two sequential proposals (control write
+ scheduler enqueue) instead of one; under divergent lane leadership the
override can land while the eval enqueue is rejected and retried by the
caller.

**Why.** The submit-time ownership arbitration (`base_count`, entry 4)
must run inside `handle_job_submit_apply`'s transaction — which is a
control-lane capture, and the single-writer ownership guard rejects
foreign-lane writes inside a capture. A scheduler-owned `task_scale` would
have forced the arbitration into a deferred reconciler (split-window
inconsistency between spec and override) or a cross-lane composite the
lanes design forbids. Overrides are operator intent, like the specs they
layer over; intent lives on Control. The scheduler only reads them.

## 6. Rollouts and scaling: scale-out applies, scale-in defers (the ECS rule)

**Call.** During an active rollout, a scale-out takes effect on the next
rollout tick (step targets recompute from effective count); a scale-in is
recorded and applied when the rollout completes.

```
$ legion job scale web --count 2
deferred: rollout web-7 is in progress; scale-in to 2 applies when it completes
```

**Alternatives.** (a) Nomad post-#14669: reject scaling during deployments
outright (400) — simplest and race-free, but you cannot add capacity during a
deploy, which is precisely when launches need it. (b) Allow both directions
immediately — maximal responsiveness, and reintroduces the mid-rollout
retirement races Nomad hardened against.

**Trade-off.** Scale-in latency during long rollouts (bounded: it applies at
completion), and the rollout engine's step math must tolerate targets growing
between ticks — a satisfied step can become unsatisfied, which tests must pin
as intended behavior rather than a regression.

**Why (simplicity + performance).** Mechanically nearly free — rollout ticks
already recompute desired from config each pass, so reading effective count
gets scale-out for free — and it is the behavior ECS shipped as the
operationally-correct default: capacity up always, disruption down never
mid-deploy.

## 7. Deferred scale-in is a `pending_count` column, promoted on terminal rollout status

**Call.** (Implementation, 2026-07-30.) A scale-in during an active rollout
writes `task_scale.pending_count` (leaving any applied `count` untouched);
every terminal rollout transition (`completed`, `failed`, `cancelled`,
`completed_incomplete` — including operator `rollout stop`) promotes
pending → applied and enqueues the `job_scale` eval. Effective-count
readers ignore `pending_count` entirely.

**The alternatives.** (a) Write the override immediately and let rollout
ticks see the reduced count — but ticks recompute targets from effective
count, so the "defer" rule (entry 6) would be a lie: targets would shrink
mid-rollout. (b) Reject scale-in during rollouts (Nomad's 400) — entry 6
already rejected that. (c) Keep the pending intent in manager memory —
lost on failover, silently dropping an acknowledged operator action.

**The trade-off.** A second nullable column with promotion plumbing on
every terminal rollout path, and a rollout cancelled by a RESUBMIT starts
a new rollout immediately — the pending scale-in then waits for *that*
rollout too (consistent with the rule, but a user watching may be
surprised; the scale-status surface shows the pending count).

**Why.** It is the only shape where the recorded intent is durable
(replicated row), the defer rule is actually true (readers never see the
pending value), and promotion is exactly once (the UPDATE ... RETURNING
flips pending to applied atomically).

## 8. Node scale-up keys on unplaceable work, not a utilization threshold

**Call.** The primary cluster-scaling signal is the scheduler's own placement
shortfall (`required − placed`, with the failing constraint/resource shape),
matched against node-group templates; utilization headroom is a secondary,
optional target. Scale-in is request-based (`empty`, then `least_busy`).

**Alternatives.** Nomad's model: strategy plugins over utilization metrics
(percentage-allocated CPU/memory via an APM), the operator picks thresholds.

**Trade-off.** Shortfall-driven scale-up is reactive — a node is requested
only after work fails to place, so cold-capacity latency is the provider's
boot time (softened by the optional `headroom` target and, later, the artifact
pre-warm plan). We also take on shortfall bookkeeping in the scheduler, a new
hot-path concern the benchmarks must gate.

**Why (simplicity + performance).** Pending work is a crisp, self-calibrating
signal: no threshold to tune, no APM dependency, and it works from zero nodes
(Nomad's utilization queries cannot — no nodes, no metric). This is the CA/
Karpenter lesson, and it composes with Legion's existing eval machinery
instead of a parallel metrics pipeline.

## 9. Node providers speak the exec ABI, not dylibs

**Call.** The provider contract (`status` / `scale_up` / `terminate`) is a
volume-style exec plugin: op as argv, JSON on stdin/stdout, timeout. v1 ships
`exec` (bring your own script) and `noop`; a first-party `aws-asg` follows as
its own unit.

**Alternatives.** Dylib providers like the five other plugin kinds — richer
interface, no process-spawn overhead, consistent with the majority pattern.

**Trade-off.** Exec ops are slower per call and the interface is stringly
(mitigated: provider calls are seconds-scale cloud API operations at
minutes-scale cadence, where process overhead is noise). Two provider ABIs in
the codebase — but volumes already made that split for the same reason, so
this follows precedent rather than adding a third style.

**Why (extensibility + the security boundary).** Providers hold cloud
credentials, make network calls, and hang; a crashed or wedged provider must
not take the manager with it (dylib panics kill the host —
`docs/plugin-system.md` says so plainly). Exec also means day-one users glue
in Terraform or the `aws` CLI with a shell script instead of waiting for us to
compile their cloud's SDK into a dylib.

## 10. Scaling gets its own ACL resource instead of riding `job:write`

**Call.** New `scale` resource (read/write, job-name scope). The split-out
agent's recommended policy is `scale:write + job:read + node:read`, explicitly
not `job:write`.

**Alternatives.** Reuse `job:write` — zero vocabulary growth, and any token
that can scale can also submit arbitrary jobspecs.

**Trade-off.** The closed resource list grows by one, every operator-facing
policy doc gets a new noun, and pre-existing admin policies written as
`job:*` don't automatically cover scaling (called out in the upgrade notes).

**Why (security boundary → simplicity of delegation).** "May resize, may not
redeploy" is the exact shape both an autoscaler credential and a
capacity-oncall teammate need; without the resource, least privilege for the
agent job is impossible and a compromised autoscaler is a full deploy
credential. Cheap now, breaking later.

## 11. Usage samples ride the heartbeat's existing status write; windows live in evaluator memory

**Call.** Per-allocation CPU/memory lands as optional fields on
`AllocationHealth`, widening the `allocation_health` last-value row the
heartbeat already writes on the Status lane. Smoothing/stabilization windows
are in-memory rings in the evaluator; after leader failover the loop refuses
to scale in until one full window elapses.

**Alternatives.** (a) A new time-series table with retention — real history,
UI sparklines, windows survive failover. (b) A parallel unreplicated in-memory
path on the receiving manager — no storage cost at all, but the evaluator
leader and the heartbeat receiver aren't guaranteed to be the same manager.

**Trade-off.** No usage *history* anywhere durable (the UI shows now, not last
week — an external Prometheus scraping a future aggregate endpoint is the
history story), heartbeat rows get wider on a hot path (benchmarked), and
failover conservatism means scale-in pauses ~5 minutes after elections.

**Why (performance + simplicity).** Zero new write paths on the per-heartbeat
critical path and no retention/pruning subsystem to operate, while replication
keeps the split-out agent and every manager able to read the same last values.
The failover pause errs in the safe direction by construction.

## 12. `min` defaults to 1, and `min: 0` must name its wake signal

**Call.** Scale-to-zero is opt-in twice: `min: 0` plus at least one check with
an `activation` threshold on a source that exists at zero replicas. Validation
rejects the deadlock case:

```
error: scaling.min = 0 requires a check that can wake the task from zero.
  'cpu' reads task usage, which is empty at count 0.
```

**Alternatives.** (a) Nomad: allow `min = 0` with no activation path — the
task sleeps forever and the operator learns why from a forum post. (b) Follow
Nomad's `min` default (= spec count), which can never scale below what the
spec asked for — arguably safer, but it makes the common "idle down to 1"
case require explicit config, and HPA's `min = 1` is the mainstream
expectation.

**Trade-off.** Wake-on-request is honestly absent until the ingress exists
(Fly's model, reserved as the seam) — so v1 scale-to-zero serves queue/cron
workers, not request-serving apps, and the docs must say so plainly.

**Why (simplicity/ease).** A validation error at submit is the cheapest place
to teach the KEDA lesson that 0→1 is a different mechanism from 1→N; shipping
a knob that silently strands workloads at zero would spend the simplicity
budget on support threads instead.

## 13. UI: first-party pages now, a link-only extension primitive, panels deferred

**Call.** The autoscaler's UI is native SPA pages. The only generic mechanism
in this plan is `ui: { link: ... }` — an "Apps" sidebar section linking out to
a job's own served UI via the service catalog. Third-party *panels* inside the
operator UI are a named non-goal pointing at a future `ui-extensions-plan.md`.

**Alternatives.** Ship the full mechanism now (sandboxed iframe, scoped
short-lived tokens, postMessage contract) so the autoscaler dogfoods it — the
user's stated ideal, and it would prove the extension story early.

**Trade-off.** The autoscaler doesn't exercise the extension mechanism, so the
first real consumer of `ui-extensions` arrives later with no dogfooding; and
link-out UX is weaker (separate origin, must be browser-reachable — LAN/dev
today, universal only once the ingress lands).

**Why (security boundary + simplicity).** Embedded third-party JS in the
origin that holds the operator's bearer token (`localStorage` today) is
XSS-by-design without origin isolation and token scoping — a real security
design, not a feature flag, and bolting it half-safe onto an autoscaler plan
is how it ships wrong. The link primitive delivers most of the discoverability
for none of the attack surface, and being first-party, the autoscaler never
needed the mechanism anyway.

## 14. v1 refuses `scaling` on keyed and `count.per` tasks

**Call.** Validation rejects a `scaling` block on keyed-stateful tasks (no
count to drive; instance lifecycle is the API's) and on `count: { per: ... }`
tasks (what does "scale out" mean per attribute group? — undesigned).

**Alternatives.** Define it now: keyed scaling driving
`CreateInstances`/`DestroyInstance`, and per-group scaling multiplying `num`
per group.

**Trade-off.** Two visible feature holes, each with users who will eventually
want them (the stateful plan already deferred keyed autoscaling once).

**Why (simplicity/ease).** Both need semantics this plan would be guessing at
— which key dies on scale-in? do groups scale independently or in lockstep? —
and a loud validation error is reversible while a guessed default is not. The
checks/strategy machinery reuses cleanly whenever the answer is designed, so
deferring costs mechanism nothing.

## 15. Scaling does not move the job's `updated_at`

**Call.** (Implementation, 2026-07-30, surfaced merging with the
job-mtime work.) `legion job scale` writes the override row's own
`updated_at` but leaves `job.updated_at` alone, so the UI's "Updated"
column and `ListJobs sort_by: updated_at` do not move when a job is
scaled.

**The alternative.** Touch `job.updated_at` on every scale — a scale *is*
a change to the job's effective shape, and an operator scanning the jobs
list for "what changed recently" would arguably want to see it.

**The trade-off.** The jobs list can show a job as untouched for weeks
while its replica count moved this morning. The recency signal for
scaling lives elsewhere: the `scale.applied` event, and the override
row's timestamp via `legion job scale <name>`.

**Why.** The job-mtime plan defines that column narrowly and explicitly —
"it moves for spec submissions and status transitions, and *only* for
those" ([job-updated-at-plan.md](../plans/job-updated-at-plan.md) §4) —
and this feature's entire premise (entry 1) is that scaling is *not* a
spec submission. Bumping it would make the count override behave like a
version bump in the one surface where users read version churn, undoing
the distinction both plans exist to draw. Revisit if operators report
missing scale activity in the jobs list; the cheap fix is a scale column
there, not overloading the mtime.
