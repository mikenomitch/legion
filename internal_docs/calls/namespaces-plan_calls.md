# Judgement Calls: Namespaces

Plan: [`plans/namespaces-plan.md`](../plans/namespaces-plan.md).

**Entries are ranked by project impact, highest first.** Impact means how
much of the system the decision constrains, how expensive it would be to
reverse, and how visible it is to users. The entry number is its rank.

Most entries were made while *writing* the plan. Entries marked
**Implementation call** were made while building it (2026-10-19), each
inserted at its rank; plan-time entries that implementation changed say
**Revised** or **Reversed during implementation** and keep their original
reasoning below the note.

**Revision history.** The first set of calls was written on 2026-09-29. They
were revised on 2026-10-18, after two changes:

- AGENTS.md gained "Legion has no installed base to protect" (2026-10-11).
- A sourced survey of Kubernetes and Nomad namespace failures was done (plan
  § 2–3).

Entries marked **Reversed** or **Revised** keep what the earlier version
decided and say why it changed. The superseded reasoning is part of the
record, not deleted.

---

## 1. Job identity is the qualified id `namespace/name`, minted once, with per-namespace uniqueness from the first phase

**Reversed** (2026-10-18). The earlier version split this into two phases.
Phase 1 was to stamp the namespace and make lookups namespace-scoped while
names stayed globally unique. Phase 2 was to drop the global unique index.
The point was that the one behavioral change would land while every job was
still in `default`, where no one could observe it. That reasoning protected
an installed base that doesn't exist. And recon found that the plan's
premise, "the foreign-key graph does not need to change", was false:
`job.id` *is* the job name (`src/storage/store.rs:10545`), so every
`job_id` column stores the bare name.

**Revised during implementation** (2026-10-19). Two refinements, both
visible on the wire:

- **The canonical id omits `default/`.** A job in `default` keeps its bare
  name (`web`); elsewhere it is `payments-prod/web`. A cluster that never
  creates a namespace has exactly the ids, derived keys, and seal-key inputs
  it had, which is what made call 18's refusal migration unnecessary.
- **There are no separate `namespace` and `name` columns.** The namespace is
  derived from the id by one total split (`split_job_id`: no `/` means
  `default`), so nothing can store the two disagreeing. Authorization still
  works on the split fields (`AclScope::job(id)` sets `namespace` and `name`),
  never on string prefixes.

The cost: a script that splits on `/` must read a bare id as `default`, and
a path that wants the namespace as its own segment must split the id (call
11).

**Call.** `job.id` becomes `"{namespace}/{name}"`, minted at submit, with
`namespace` and `name` also kept as their own columns. Every key derived
from a job id inherits the namespace with no further change:

```text
job.id        = "payments-prod/web"
job_config.id = "payments-prod/web-4"
seal key      = HKDF(..., "payments-prod/web")
manifest      = manifests/payments-prod/web/<group>/<task>/<instance>.json
```

Per-namespace names are allowed in the same phase that introduces
namespaces.

**Alternatives.**

- (a) Keep the two-phase split. It gives smaller diffs and an unobservable
  migration.
- (b) A surrogate id (a UUID) with a unique `(namespace, name)` index. That
  is the textbook relational answer, and it would make a future rename
  possible.
- (c) Keep `job.id` as the bare name and thread a `namespace` column through
  every job-keyed table and every derived-key function.

**Trade-off.**

- `job_id` changes shape on the wire, in paths and in every fixture.
- Every site that treats `job_id` as one path component has to take two.
- Phase 1 gets large: identity, services and minimum UI routing have to
  land together.
- The qualified string looks like option D, the prefix-as-name design the
  plan rejects. It isn't, but only as long as nothing parses it for
  authorization (plan § 8.2), and that invariant has to be defended in
  review.
- (b) would have kept rename possible. This call closes it (call 17).

**Why (simplicity and security).** Nomad shipped namespaces with client-side
keys and paths that lacked them. It then fixed same-id volumes failing to
stage, and a CSI path missing the namespace, one key at a time
(changelog GH-20532, GH-25253). Legion has about thirty places that derive
something from a job id, including checkpoint seal keys. Changing the one
place ids are minted fixes all of them. Threading a column through all of
them (c) leaves a cross-tenant collision behind every one that is missed.
(b) buys rename, which nobody gets in Kubernetes, Nomad or Temporal Cloud,
at the cost of an extra id people must map back to a name in every URL and
log line. (a) adds an extra migration to protect clusters that don't exist.

---

## 2. Namespaces are explicitly not an isolation boundary, and the docs lead with a plane-by-plane table

**Revised** (2026-10-18). The call is unchanged. It now has sources, and the
plane table (plan § 8.6) names two gaps found in recon:

- Node caches serve private images without checking credentials
  (`src/artifacts/fetchers/oci.rs:611, :701`).
- Cluster-scoped views such as `node:read`'s ledger expose every tenant's
  job ids.

**Call.** A namespace is a naming, authorization and governance boundary.
Isolation comes from elsewhere:

- Compute: node pools.
- Network: `network.mode = "gated"` plus net policies.
- Hostile tenants: the `sandbox` runtime, and later federation.

The non-goal comes first in `docs/namespaces.md`, in the proto comment on
`Namespace`, and in `legion namespace --help`. It is shown as a table of
what a namespace does and does not do on each plane.

**Alternatives.**

- (a) Make namespaces do isolation work: bind each namespace to a pool, or
  imply `gated` networking for everything in it.
- (b) Say nothing, and let users infer.

**Trade-off.** Users arriving from Kubernetes will ask why `prod` doesn't
keep its work off `dev`'s machines. The answer is a second concept (pools,
then allowed pools in namespace policies), not a property of the first. It
costs one extra explanation per new operator.

**Why (simplicity).** The sources agree:

- The Kubernetes multi-tenancy docs and the EKS guide ("Kubernetes is a
  *single tenant orchestrator*") both say a namespace isolates very little.
- Consul needed admin partitions on top of namespaces.
- Nomad had to add node pools in 1.6 because namespaces never controlled
  placement ([#11041](https://github.com/hashicorp/nomad/issues/11041)).

(a) also has its own failure: a namespace that silently rewires a workload's
networking or placement makes the tenancy boundary the thing that breaks a
deploy. Legion already has the isolation primitives, so it can say what a
namespace is *for* without leaving anyone stranded.

---

## 3. A rule over a namespaced resource must name its namespaces; `["*"]` is the explicit "all"

**Reversed** (2026-10-18). The earlier version kept "an omitted `namespaces`
selector means every namespace". It made the hazard visible with warnings at
`acl policy apply` and a blast-radius report at `namespace create`. The
argument was that the engine's single rule ("a rule without a selector
matches any value") must have no exceptions. That entry rejected "reject job
rules without `namespaces` once more than one namespace exists". Rejecting
it was right, because a policy that becomes invalid when someone else
creates a namespace is worse than either option. The survey then showed that
both incumbents make "all namespaces" an explicit choice, and that the
warnings were the only line of defense.

**Call.** `parse_rules` enforces two things.

1. A rule whose resources include any namespaced resource (`job`, `exec`,
   `action`, `scale`, `boot`, `snapshot`, `namespace`) must carry
   `namespaces`.
2. A rule over only cluster-scoped resources must not carry it.

```console
$ legion acl policy apply ci --rules @ci.ncl
error: rule 0 (job:write) must say which namespaces it covers
hint: add namespaces = ["default"] for one namespace, or ["*"] for every
      namespace, including ones created later

error: rule 2 (node:read) has a `namespaces` selector, but node is cluster-scoped,
       so the rule could never match
```

**Alternatives.**

- (a) The earlier call: omitted means all, with warnings and a blast-radius
  report.
- (b) Omitted means `default` only. That is safe by default, but it makes
  one selector mean something different from every other one.

**Trade-off.** Every job policy now has to mention namespaces, including on
a single-team cluster that will never have a second one:
`namespaces = ["*"]` becomes boilerplate there. That cuts against call 23
(a single-team cluster shouldn't have to learn the concept). It is accepted
because the people writing ACL policies are the admin persona, the error
teaches the fix in one line, and it is the exact moment tenancy matters.

**Why (security over brevity).** Kubernetes grants all-namespace reach only
through a ClusterRoleBinding someone chose, and wildcards there grant "all
object types which are created in the future"
([RBAC good practices](https://kubernetes.io/docs/concepts/security/rbac-good-practices/)).
Nomad grants it only through a `namespace "*"` block someone wrote. In both,
a forgotten scope grants *less*, not everything. Under (a), a policy written
for a one-namespace cluster silently reaches every namespace created next
year, and the only defense is someone reading a warning.

This call keeps the engine's sentence true: omitted still means any. It just
won't let a namespaced rule leave this one out. The second check also
removes a class of silent no-op rules for free. Today `namespaces` on a
`net` rule matches nothing (`src/manager/server.rs:13518`), and so does
`namespaces` on `kv`.

---

## 4. A stored rule without `namespaces` reaches `default` only; `*`-resource and deny rules are exempt from the selector requirement

**Implementation call** (2026-10-19), refining call 3.

**Call.** Two refinements to the "a namespaced rule must name its
namespaces" rule:

- **Validation exemptions.** `resources = ["*"]` rules and deny rules may
  omit `namespaces`. A rule mixing namespaced and cluster-scoped resources is
  refused (split it), as is an empty selector:

  ```text
  rule 1 mixes namespaced resources (job) with cluster-scoped ones (node);
  split it into two rules, one with a `namespaces` selector and one without
  ```

- **Evaluation of legacy rules.** An allow rule that names namespaced
  resources explicitly but carries no `namespaces` (only a rule stored before
  this release can, since validation refuses to write one) matches the
  `default` namespace only. A `*`-resource rule and a deny rule without the
  selector keep matching every namespace. The UI's ACL mirror implements the
  same rule.

**Alternatives.**

- (a) Keep the engine's "a missing selector matches any value" for
  `namespaces` too, and rely on validation for new rules.
- (b) Refuse to start a manager whose stored policies hold such rules.
- (c) Require `namespaces` on `*` rules and deny rules as well.

**Trade-off.** The engine's one-sentence rule ("no selector matches any
value") gains an exception for one selector. A legacy `job` rule that an
operator expected to follow jobs into new namespaces does not, until it is
re-applied with `namespaces = ["*"]`, and the upgrade note has to say so.

**Why (security, then simplicity).** With (a), every policy written before
namespaces (`job:write names = ["web-*"]`) silently widens to every namespace
created since, which is exactly the "grant that follows the tenant around"
the plan exists to prevent, delivered by an upgrade. (b) breaks every
upgraded cluster at boot for a fix that can be applied lazily. Defaulting to
`default` preserves what the rule meant when it was written, because every job
lived there. (c) would force `namespaces = ["*"]` onto the admin's
`{ resources = ["*"], actions = ["*"] }` rule, which already says "everything"
as explicitly as anything can, and onto deny rules, where a missing selector
can only make a denial broader.

---

## 5. Server job fields take the canonical id; only spec-bearing requests and list selectors carry a `namespace`

**Implementation call** (2026-10-19). Deviates from plan § 11, which gave
every request that names a job a `string namespace` field.

**Call.** A job's canonical id is the bare name in `default` and
`namespace/name` elsewhere, and every server field that names a job
(`name`, `job_id`, `job`) takes it. Clients resolve `-n`,
`LEGION_NAMESPACE`, and `ns/name` before calling. The only requests with a
`namespace` field are those that carry a spec (`RunJob`, `PlanJob`,
`RunJobTemplate`, `RenderJobTemplate`), where the server must reconcile it
with the spec's own; lists take `repeated string namespaces`.

```text
GetJobInfo { name: "payments-prod/web" }      # not { name: "web", namespace: "payments-prod" }
```

**Alternatives.** (a) The plan's shape: a `namespace` field on every
job-naming request, with the name field accepting either form and a shared
server-side parser rejecting disagreement.

**Trade-off.** API callers must compose the id themselves (`ns/name`, or the
bare name in `default`), and a raw `grpcurl` user cannot just add a field.
Canonical ids also omit `default/`, so a script splitting on `/` must treat
a bare id as `default`.

**Why (simplicity).** Dozens of RPCs name jobs. (a) adds a field and a
disagreement rule to each, and every handler must remember to resolve before
it scopes; one that forgets authorizes against the wrong namespace. With one
id on the wire, the id a reply returns is the id the next request takes,
authorization splits that one string the same way everywhere
(`AclScope::job(id)`), and only the four requests that genuinely hold two
sources of truth (spec and request) carry two fields. Omitting `default/`
means a cluster that never creates a namespace has the same ids, keys, and
paths it always had.

---

## 6. Services are namespaced in the same phase as jobs, and crossing is explicit at both ends

**Call.** The earlier version deferred the namespaced catalog to Phase 3.
Here it is part of Phase 1:

- The catalog is keyed `(namespace, service)`.
- `connect` and `<service>.svc.legion` resolve in the consumer's own
  namespace.
- A bare `allow` name never reaches outside its namespace.
- The three service-read RPCs filter per item.

Crossing (Phase 2) needs the consumer to name the namespace *and* the target
to admit the consumer by qualified id:

```nickel
connect = { upstreams = [ { service = "auth", namespace = "shared" } ] },   # consumer
service = { port = 8443, allow = ["checkout-prod/api", "payments-*/api"] }, # target
```

**Alternatives.**

- (a) The earlier phasing: namespaces first, discovery later, with the
  public doc warning that discovery isn't isolated yet.
- (b) Kubernetes' default: names resolve across namespaces by qualified DNS,
  with no target-side consent.
- (c) Consumer-side opt-in only.

**Trade-off.** Phase 1 grows by a subsystem. Shared platform services
(`shared/auth`) aren't reachable from tenants until Phase 2. Teams that need
them in the meantime have to keep them in the consumer's namespace.

**Why (correctness, then security).** Recon showed that today's catalog
*pools* same-named endpoints across jobs (`src/storage/store.rs:15066`). So
under (a), namespaces would ship with team A's `connect` upstream
load-balancing onto team B's instances. That isn't a visibility gap, it is
misrouted traffic. It is exactly the failure Nomad users hit on Consul CE:
"Services with the same name in different namespaces appear as a single
service… with 2 instances"
([Discuss 16712](https://discuss.hashicorp.com/t/nomad-oss-namespace-and-service-discovery/16712)).

Both-ends consent is Gateway API's ReferenceGrant lesson. That design exists
because one-sided cross-namespace references were "a prime source of
privilege escalation attacks"
([#17088](https://github.com/kubernetes/kubernetes/issues/17088)), and
CVE-2021-25740 is one. Designing the crossing in now also answers the most
upvoted Nomad namespace issue
([#14177](https://github.com/hashicorp/nomad/issues/14177)) up front,
rather than six years later.

---

## 7. Quota charges the larger of declared and live reservations, inside the control-lane submit; there is no plan-apply re-check

**Implementation call** (2026-10-19). Deviates from plan § 8.12, which
checked quota at submit against usage and re-checked at plan apply so racing
submits could not both squeeze under the cap.

**Call.** Per band, a job is charged the larger of what it declares (for
count-driven tasks of its current config, the effective count times the
reservation) and what its live allocations reserve, dimension by dimension.
The quota and `maxJobs` checks run inside the replicated control-lane write
that stores the job, so racing submits serialize. A job never loses ground
it holds: a resubmit that does not grow in a dimension passes it even over
the cap.

```console
error: namespace policy 'payments' caps band 'high' at 64000 MHz CPU across payments, payments-*;
       in use 61500 MHz CPU, this job adds 4000 MHz CPU
```

**Alternatives.**

- (a) The plan: charge live allocations at submit, and re-check in the
  scheduler's plan apply, refusing placements over the cap.
- (b) Charge declared demand only.

**Trade-off.** A job whose allocations are pending or failing still holds its
declared quota. Keyed instances, per-node tasks, and the old version during
a rollout are charged only once they run, so their admission is a pre-check
(instance create, scale-out) that two racers can both pass. Each governed
submit scans the cluster's jobs and live allocations inside the control-lane
entry, linear in cluster size.

**Why (performance, then simplicity).** (a) puts a namespace-usage
computation on the plan-apply path, the scheduler's hottest write, for every
placement in a governed namespace, and turns a quota refusal into a blocked
eval the submitter never sees. Charging declared demand closes the race at
admission instead, where Borg puts it ("quota-checking is part of admission
control, not scheduling"), and where the user gets the error. (b) would let
keyed and per-node work run uncharged; taking the max with live usage is
what covers them.

---

## 8. `{namespace}` joins the workload-identity uniqueness floor and the default subject, with no pin-the-old-template option

**Revised** (2026-10-18). The earlier version changed the default template
in the phase that relaxed name uniqueness. Clusters that couldn't take the
change could pin the old template, getting a startup warning once a second
namespace existed. Without an installed base there is nothing to pin for.
Recon also found that templates already have a hard uniqueness floor that
rejects a template missing `{job}`, `{group}` or `{task}`
(`src/structs/workload_identity.rs:543`), which makes the pin option
inconsistent as well as unnecessary.

**Call.**

- The default becomes `{region}:{namespace}:{job}:{group}:{task}`, which
  renders as `global:payments-prod:web:frontend:nginx`.
- `validate_sub_template` rejects any cluster, allowed or per-identity
  template that omits `{namespace}`, with the message shape it already uses
  for `{job}`.
- Tokens carry a `legion.io/namespace` claim that binding rules can match.

**Alternatives.**

- (a) Keep the namespace out of the default and let operators opt in.
- (b) Put it in the default but allow templates without it, with a warning.

**Trade-off.**

- Every subject in existence changes shape, so IdP trust policies that match
  `sub` are rewritten.
- A team that wanted short subjects on a one-namespace cluster can't have
  them.

**Why (security).** With per-namespace names and a namespace-free subject,
`payments-prod/web` and `checkout-dev/web` mint **identical** JWT subjects.
An IdP policy that trusts production would then accept development's tokens.
That is a forgery path created by a feature whose whole purpose is
separating tenants. The floor exists precisely so that "distinct workloads
stay distinct", and dropping the namespace is the same violation as dropping
the job. Nomad's composed subject includes the namespace, and its
integrations bind Vault and Consul roles on `nomad_namespace`
([workload identity docs](https://developer.hashicorp.com/nomad/docs/concepts/workload-identity)).

---

## 9. Governance lives in namespace policies that select namespaces by glob and cap them in aggregate; priority bands stay in ACL rules

**Reversed in part** (2026-10-18). The earlier version put quota,
`allowedPools` and `allowedRuntimes` as fields **on the namespace object**.
The research broke that twice:

1. **Delegated creation dodges a per-namespace quota.** If `payments-*`
   creation is delegated (call 12), a quota on `payments-prod` is escaped by
   creating `payments-prod2`. That is why OpenShift (ClusterResourceQuota),
   Rancher (project limits), Kiosk (AccountQuota) and HNC
   (HierarchicalResourceQuota) all added quotas spanning namespaces.
2. **The namespace writer becomes the quota writer.** That is Kubernetes'
   namespace-label escalation, where a namespace owner loosens their own Pod
   Security level, in a new place
   ([RBAC good practices](https://kubernetes.io/docs/concepts/security/rbac-good-practices/)).

The half that keeps bands in ACL rules is unchanged.

**Call.** Phase 4 adds namespace policies, which are cluster objects shaped
like net policies:

```nickel
{ name = "payments", namespaces = ["payments", "payments-*"],
  quota = { high = { cpu = 64000, memory = "128Gi" } },     # per band, aggregate
  maxNamespaces = 8,
  pools = { allowed = ["shared-*", "payments-gpu"], default = "shared-general" },
  runtimes = ["container", "sandbox"] }
```

- Several policies can match one namespace, and all of them apply. Each
  quota is checked independently, and allowlists intersect.
- They are checked at admission, with a re-check at plan apply.
- Writes are gated on `acl:write`.
- The namespace object keeps only `description` and `meta`.

**Alternatives.**

- (a) The earlier call: fields on the namespace.
- (b) A separate per-namespace quota object, Kubernetes ResourceQuota-style,
  which fixes the writer problem but not the dodge.
- (c) Move band caps here too, giving one governance surface.

**Trade-off.**

- It is a new object kind, so there is one more noun to learn.
- "Why are my limits not on my namespace?" needs an answer in the docs.
- Effective limits are computed from every matching policy, so the CLI has
  to show them (`legion namespace get`) rather than letting users read them
  off one object.

**Why (simplicity, measured against what the field rebuilt).** Every
Kubernetes tenancy product rebuilt an aggregate quota on top of the
per-namespace one. Starting with the aggregate means not shipping the thing
everyone replaces. A glob over immutable names is Legion's existing grouping
mechanism, and it is evaluated live. So there is no "defaults don't
backfill" (Rancher), and no namespace↔quota deletion cycle (Nomad
[#23672](https://github.com/hashicorp/nomad/issues/23672)).

Per-band quota is Borg's model: "quota… at a given priority", "every user
has infinite quota at priority zero", "part of admission control, not
scheduling". It maps directly onto Legion's six bands.

Bands stay in ACL rules because the band question is per principal ("the
autoscaler may claim `high`, humans may not"). Moving them would recreate
the `priority.maxBandByNamespace` config map that
[preemption call 8](preemption-and-eviction-plan_calls.md) rejected.

---

## 10. Instance keys are qualified by their job's namespace; minted ids stay bare

**Implementation call** (2026-10-19). Plan § 8.2 made the instance primary
key `"{namespace}/{instance_id}"`; this keeps that for keys and refines it.

**Call.** A caller-chosen key is stored as its canonical id, `user-42` in
`default` and `payments-prod/user-42` elsewhere, so keys and tombstones are
unique per namespace. Server-minted ids (`<task>-<26 base32>`, node-tagged
session ids) stay bare. The CLI and MCP resolve instance arguments like job
arguments; a minted id is used as written. Tasks get `LEGION_INSTANCE_ID`
(canonical) and `LEGION_INSTANCE_KEY` (the key).

**Alternatives.** (a) Qualify every id, minted ones included. (b) Keep ids
cluster-unique and only hide the collision message.

**Trade-off.** Two forms of instance id exist (qualified keys, bare minted
ids), and a client resolving a bare argument has to recognize the minted
shape. `LEGION_INSTANCE_ID` now contains a `/` for namespaced keyed
instances, which a workload using it as a file name must handle (hence
`LEGION_INSTANCE_KEY`).

**Why (security, then simplicity).** (b) leaves the cross-tenant existence
oracle and name squatting in place: tenant B could learn that A holds
`user-42`, or take `acme` first. (a) would rewrite every minted-id parser
(session routing reads the node tag out of the id) for ids that are already
unique by 130 random bits.

---

## 11. Content-store paths spell the namespace as its own segment, `default` included; the fence segment becomes `_fence`

**Implementation call** (2026-10-19). Plan § 8.2 showed
`manifests/payments-prod/web/...` and assumed the qualified id would carry the
namespace into paths for free.

**Call.** Archive and session manifests are written under
`manifests/<namespace>/<job>/…` and `sessions/<namespace>/<job>/…`, with
`default/` spelled out, and archive files are named by the instance key. The
session fence prefix moves from `sessions/fence/` to `sessions/_fence/`.

**Alternatives.** (a) Use the canonical id as written:
`manifests/<job_id>/…`, which is `manifests/web/…` in `default` and
`manifests/payments/web/…` elsewhere.

**Trade-off.** Every existing archive and store-session manifest moves path,
and an upgrade does not find the old ones (the upgrade note says to resume
them first).

**Why (correctness).** With (a), default job `web`'s listing prefix
`manifests/web/` covers namespace `web`'s jobs (`manifests/web/api/…`), and a
namespaced job's slot instance id adds a path segment that the parser
rejected. A namespace named `fence` would likewise put its manifests under
the fence lister's prefix; `_` is outside the namespace grammar, so no
namespace can.

---

## 12. Namespace CRUD is a delegable ACL resource, and a cascade delete also needs `job:write`

**Revised** (2026-10-18). The earlier entry argued only "not `operator`".
The survey added the case for delegation and the escalation it must not
open.

**Call.** A new `namespace` resource is scoped by `namespaces`, so a team
lead can be given their own environments:

```nickel
{ resources = ["namespace"], actions = ["write"], namespaces = ["payments-*"] }
```

`--cascade` additionally requires `job:write` on the namespace's jobs.
`ListNamespaces` shows a namespace to anyone holding *any* allow in it.

**Alternatives.**

- (a) Gate namespace CRUD on `operator:write`.
- (b) Gate it on `acl:write`.
- (c) Nomad's model: management tokens only.

**Trade-off.** It adds one more resource to a list the plan elsewhere wants
kept short. Delegated creation also means namespace sprawl inside a glob. It
is capped by `maxNamespaces` in Phase 4, and uncapped until then.

**Why (the evidence).** Several sources point the same way:

- Nomad's "only management tokens can create namespaces" is an open
  complaint ([#14003](https://github.com/hashicorp/nomad/issues/14003)), and
  delegated namespace admins are item 2 on its multi-tenancy meta issue
  ([#18803](https://github.com/hashicorp/nomad/issues/18803)).
- HNC's founding complaint was that "whenever a team wants a new namespace,
  they must raise a ticket".
- `operator` means raft and manager configs, and `scale` and `boot` were
  split out for the same reason.

The cascade's extra requirement answers Kubernetes' finding that a service
account can delete its own namespace, and everything in it, with only a
namespaced wildcard binding
([#134929](https://github.com/kubernetes/kubernetes/issues/134929)). The
right to delete an environment must not become a back door to destroying
workloads you can't otherwise touch.

---

## 13. Deletion refuses while non-empty; `--cascade` is synchronous and its final step is an atomic "delete if empty"; there is no `Terminating` state

**Revised** (2026-10-18). The earlier call is unchanged, with three
additions:

- One emptiness list for all namespaced kinds.
- An atomic final step.
- No dependency on optional components.

**Call.**

```console
$ legion namespace delete payments-old
error: namespace 'payments-old' is not empty: 3 jobs, 1 volume, 2 snapshots
hint: `legion namespace delete payments-old --dry-run` lists them; --cascade removes them
```

The cascade works in a fixed order: stop, destroy, delete volumes and
snapshots, delete the namespace. The last step is one conditional store
write. A submit that races in makes it refuse, and re-running finishes the
job.

**Alternatives.**

- (a) Kubernetes: always cascade, asynchronously, with finalizers and a
  `Terminating` phase.
- (b) Refuse always, with no cascade.
- (c) Temporal: mark as deleted, rename to free the name, reclaim in the
  background.

**Trade-off.** A cascade over a big namespace blocks for as long as the work
takes, and a client timeout means re-running it. There is also no
"deleting" state to watch, only the command's own progress.

**Why (simplicity).**

- Kubernetes' Terminating namespaces are the canonical case: [#60807](https://github.com/kubernetes/kubernetes/issues/60807)
  has 119 👍 and 180 comments, and the maintainers ask people not to use the
  finalizer-stripping fix.
- Datadog lost a namespace four days after deleting it, the moment an
  unrelated metrics API recovered.
- Nomad shows the other failure: delete safety added kind by kind, including
  a delete that returned 200 and failed in the FSM
  ([#19414](https://github.com/hashicorp/nomad/issues/19414)).

A refusal is a state you can always get out of. A single emptiness list and
a conditional final write mean there is no kind to forget and no false
success.

---

## 14. A namespace conflict between the jobspec and the request is a server-side error

**Revised** (2026-10-18). The earlier version stated this as CLI
behavior. It is now a server rule, because the vulnerability it prevents was
server-side.

**Call.**

```console
$ legion job run api.ncl -n dev      # api.ncl declares namespace = "prod"
error: jobspec declares namespace 'prod' but the request names 'dev'
```

Every spec-bearing RPC applies this rule: `RunJob`, `PlanJob`,
`ValidateJob`, `RunJobTemplate`, and volume create. Authorization runs
against the resolved namespace. The CLI's qualified-name-vs-`-n` conflict is
the same rule applied one layer earlier.

**Alternatives.**

- (a) Nomad's behavior: the flag wins.
- (b) The jobspec wins.
- (c) Enforce it only in the CLI.

**Trade-off.** It is one more rejection, in a place where a precedence rule
would have "just worked".

**Why (security).** Nomad left the precedence unstated, and paid for it with
bugs ([#24039](https://github.com/hashicorp/nomad/issues/24039)) and
CVE-2024-10975. In that CVE, a user created CSI volumes in namespaces they
weren't authorized for by putting the target namespace in the spec while
passing an authorized `-namespace`
([HCSEC-2024-27](https://discuss.hashicorp.com/t/hcsec-2024-27-nomad-vulnerable-to-cross-namespace-volume-creation-abusing-csi-write-permission/71212)).
Two inputs that disagree have no correct precedence. And a rule enforced
only by the CLI protects nobody who calls gRPC directly, which includes
every agent.

---

## 15. An empty list selection means `default`, except for the event feed, where it means everything visible

**Implementation call** (2026-10-19).

**Call.** `ListJobs`, `ListServices`, `ListRollouts`, `ListVolumes` and
`ListSnapshots` with an empty `namespaces` list only `default`; `["*"]` lists
every namespace the caller can read; naming a job pins its namespace and
ignores the selection. `ListEvents` and `StreamEvents` treat an empty
selection as "every event you may see, cluster events included".

**Alternatives.** (a) Empty means every readable namespace everywhere. (b)
Empty means `default` for events too.

**Trade-off.** Two meanings of "empty" exist. A raw API caller listing
rollouts or volumes sees only `default` until it asks for more.

**Why (simplicity).** Lists of namespaced objects follow the CLI's rule
(`legion job list` shows the active namespace), so a script and a person get
the same answer, and `elsewhere_count` says what was left out. Events are
different in kind: most are cluster events (nodes, raft, the scheduler) that
belong to no namespace, so a `default`-only feed would hide what an operator
tails the feed for.

---

## 16. A crossing upstream is keyed `<service>.<namespace>`, so its env vars and DNS name show the crossing

**Implementation call** (2026-10-19).

**Call.** `{ service = "auth", namespace = "shared" }` (or `service =
"shared/auth"`) has the key `auth.shared`: `LEGION_UPSTREAM_AUTH_SHARED`,
`auth.shared.svc.legion`, and the SRV form `_auth._tcp.shared.svc.legion`.
The node resolver accepts two labels under the zone. An alias (`name`) keeps
overriding the key. The endpoint feed now carries the upstream key, which also
fixes aliased service upstreams, whose feed was keyed by the catalog name the
worker's listener never used.

**Alternatives.** (a) Keep the bare service name as the key, so a crossing
reads `LEGION_UPSTREAM_AUTH` and `auth.svc.legion`.

**Trade-off.** One task cannot spell the same crossing two ways; the key is
derived, and the DNS name is two labels where a same-namespace one is one.

**Why (simplicity and safety).** With (a), a task could not hold both its
own namespace's `auth` and `shared`'s `auth`, and nothing in the task's config
would say that a name leaves its namespace. The two-label name is the
crossing made visible where it is used, which is the plan's point (§ 8.7).

---

## 17. No namespace rename and no job move, decided up front; name reuse after deletion is allowed but made visible

**Call.**

- Namespaces can't be renamed.
- A job can't be moved between namespaces. Resubmit it to the new namespace
  and destroy the old one.
- A deleted namespace's name can be created again. `namespace create
  --dry-run` lists every policy that would reach it.

**Alternatives.**

- (a) An immutable internal id with a renameable display name, as in GCP
  projects and Azure management groups.
- (b) A first-class `legion job move`.
- (c) Tombstone deleted names so they can never be reused (GCP project ids).

**Trade-off.**

- A badly named namespace is permanent, and a reorganization means
  resubmitting.
- A recreated name inherits every grant that names it. Workloads
  resubmitted under the same names also mint the same subjects, so old IdP
  trust carries over too.

**Why (the qualified id makes move expensive, and visibility beats
tombstones).** The qualified id (call 1) is the input to checkpoint seal
keys and content-store paths. "Moving" a job would mean re-sealing its
checkpoints and rewriting its archives, and doing that wrong loses data.
Kubernetes declined rename as "pretty hard"
([#43867](https://github.com/kubernetes/kubernetes/issues/43867), 56 👍).
Nomad has no rename, and Temporal Cloud names "cannot be changed". The
research's advice was "decide up front", and this is the decision.

Tombstones protect against a different tenant reclaiming a name. Here only
someone holding `namespace:write` over that name can recreate it, and they
already hold the reach. Showing the policies that reach the name, at the
moment it is recreated, deals with the real hazard: an admin who forgot
what names `payments-old`.

---

## 18. No migration for pre-namespace data directories: the store refuses to upgrade and says so

**Reversed during implementation** (2026-10-19). There is no refusal and no
rewrite. Because the canonical id leaves `default` jobs bare (call 1), an
existing store's job ids, config ids, slot instance ids, volume claim ids and
seal-key inputs are already the post-namespace values, so the only schema
change is the `namespace` table with its `default` row. Two things do move,
and the upgrade notes say so: content-store manifest paths (call 11) and
how stored ACL rules without `namespaces` are read (call 4). The text
below is the plan-time reasoning.

**Call.** The schema migration that introduces namespaces refuses to run on
a store that already holds jobs:

```text
error: this data directory holds jobs from before namespaces (schema 1xx).
Job ids are now "namespace/name" and are not rewritten in place.
Back up your jobspecs, start the manager on an empty data dir, and resubmit.
```

**Alternatives.**

- (a) A rewrite migration: prefix `default/` onto every `job_id` column, the
  ids inside JSON blobs (`allow` lists, events), and slot-instance and
  config ids.
- (b) Read both forms forever, treating a bare id as `default/`.

**Trade-off.** Every development and benchmark cluster gets wiped once.
Running workloads on a cluster being upgraded are lost, unlike every other
Legion upgrade, which `docs/upgrades.md` describes as rolling.

**Why (simplicity, per AGENTS.md).** Some derived state lives *outside* the
store and can't be migrated by SQL at all: checkpoint seal keys derived from
the bare id, and content-store paths on workers. So (a) would be a
migration that is correct for SQL and silently wrong for checkpoints. (b) is
the "dual code paths for the old form" AGENTS.md rules out. With no installed
base, a loud refusal at startup is the honest version of the break, and the
PR description says so.

---

## 19. Runtimes, pools and host volumes are checked before the submit, and a policy's default pool is written into the stored config

**Implementation call** (2026-10-19).

**Call.** The static policy rules need only the jobspec, so they run in the
submit handler before the store write; a task whose permit names no pool gets
`pool = "<pools.default>"` written into its stored permit. Two governing
policies with different defaults make such a task unadmittable. A permit
using another operator for `pool` (`!=`, `matches`) is refused when a policy
restricts pools. `job scale` out and instance create are quota pre-checks.

**Alternatives.** (a) Evaluate pools at placement (a scheduler constraint
derived from the policy). (b) Leave the default pool implicit, applied when
the scheduler reads the permit.

**Trade-off.** A policy change does not rewrite stored configs: a job keeps
the default pool it was admitted with until resubmitted. A policy applied
between the static check and the write is not seen by that submit.

**Why (simplicity and performance).** Placement is unchanged (a non-goal of
the plan), so no scheduler path learns about policies, and the stored spec
says where a task runs, which `job spec` and `job plan` then show. Refusing
an unprovable pool rule is the price of checking at admission rather than in
placement.

---

## 20. Job templates stay cluster-scoped submit channels; running one lands the job in a target namespace

**Reverses the earlier open-question lean.** The earlier plan leaned toward
namespacing templates in Phase 4, with a special `shared` namespace for
platform-published ones.

**Call.** Templates remain cluster objects, gated by the cluster-scoped
`template` resource. `RunJobTemplate` takes a target namespace. The job
scope it produces (`job_via_template`) carries that namespace, so
`{ resources = ["job"], templates = ["render"], namespaces = ["payments-*"] }`
means "may run `render` into payments namespaces". The collision guard
becomes namespace-local.

**Alternatives.**

- (a) Namespaced templates plus a `shared` namespace.
- (b) Namespaced templates plus a `shared = true` flag.

**Trade-off.** A tenant can't author private templates without being
granted `template:write` on a cluster-wide name glob (`payments-*`). That
is a naming convention in the one place this plan otherwise removes them.

**Why (simplicity).** Templates are the platform's admission-control
surface, like ClusterRoles and IngressClasses in Kubernetes. Being
published *for* tenants is their whole point. Nomad shows what
namespace-only objects cost: "No object in Nomad can be created with a
wildcard namespace identifier", so shared configuration is impossible
([#28455](https://github.com/hashicorp/nomad/issues/28455)). Option (a)
invents a special namespace to undo that. If tenants ask for private
templates, a `namespace` field can be added later without breaking the
cluster-scoped ones.

---

## 21. The UI puts the namespace in detail-route paths and the switcher in the page header, with no legacy redirects

**Revised** (2026-10-18). The earlier version kept `/jobs/:name` working
through a redirect. There is no installed base of bookmarks, so it is
dropped.

**Call.**

- Detail routes are `/jobs/:namespace/:name/...`, `/services/:namespace/:name`
  and `/volumes/:namespace/:id`.
- Lists use `?ns=`.
- Global numeric-id routes (`/rollouts/:id`) are unchanged.
- The switcher is absent on cluster-scoped pages.

**Alternatives.**

- (a) A sidebar switcher with the selection in localStorage, as in the K8s
  Dashboard and Nomad UI.
- (b) A query parameter on every page.

**Trade-off.** Lists and detail pages express the namespace two ways:
a query filter on one, a path identity on the other. Every route definition
changes.

**Why (the incident matters more than the diff).** A selection that lives
in browser storage shows the recipient of a pasted link *their* namespace.
Nomad's UI shows what happens when the namespace lives in a query parameter:
navigation dropped `?namespace=` and reloads 404'd
([#12022](https://github.com/hashicorp/nomad/issues/12022)), and the exec
window lost the namespace three separate times (changelog 1.0.4, 1.5.0,
1.8.0). A path segment can't be dropped by navigation.

---

## 22. Errors say where to look without leaking: a not-found names only readable namespaces, a denial names only the caller's reach

**Implementation call** (2026-10-19).

**Call.**

```console
error: job 'api' not found in namespace 'default'
hint: a job named 'api' exists in namespace 'payments-prod'; retry with -n payments-prod

error: permission denied: job:write requires a policy granting it in this namespace
hint: this token can write job in payments-*
```

The not-found hint lists namespaces holding the name where the caller has
`job:read`. The denial hint is read off the caller's own allow rules and never
names the target.

**Alternatives.** (a) The plan's denial text, `permission denied: job:write on
checkout-prod/web`. (b) No hints.

**Trade-off.** The denial no longer says which object was refused; the
caller has to know what it asked for.

**Why (security).** Many scopes are derived from a lookup (an instance's or
rollout's owning job). Naming the target there would tell a caller which job
owns an instance it cannot read, the existence oracle the "check before you
fetch" rule exists to prevent. Hints built from what the caller can already
see cost nothing in secrecy and answer the 3am question.

---

## 23. Single-namespace identities never see tenancy UI at all

**Call.** If `readable_namespaces` has one entry, there is no switcher. The
namespace appears as a static badge on detail headers and nowhere else.

**Alternatives.**

- (a) Always show the switcher, possibly disabled.
- (b) Hide it only when the *cluster* has one namespace.

**Trade-off.** Two people on the same cluster may see different chrome. A
user who gains a second namespace sees a control appear for the first time.

**Why (simplicity).** The common case is one team on one cluster. Kubernetes
says "for clusters with a few to tens of users, you should not need to create
or think about namespaces at all". (b) uses the wrong axis: what matters is
what *this caller* can act on. Nomad's one-namespace tokens getting greyed-out
buttons and an empty topology page
([#23905](https://github.com/hashicorp/nomad/issues/23905),
[#24334](https://github.com/hashicorp/nomad/issues/24334)) shows that this
identity must be a first-class test fixture, not an afterthought.

---

## 24. `LEGION_NAMESPACE` is a default that never conflicts with a jobspec; `-n` always does

**Implementation call** (2026-10-19).

**Call.** On spec-bearing commands, an explicit `-n` is always sent, so the
manager refuses a jobspec that names a different namespace.
`LEGION_NAMESPACE` is sent only when the jobspec names none. For job and
instance arguments both resolve the same way.

**Alternatives.** (a) Treat the environment variable exactly like `-n`.

**Trade-off.** The env var and the flag behave differently on one class of
command. Template `render` and `run` are the exception: the CLI cannot see a
template's rendered spec before the manager does, so it sends
`$LEGION_NAMESPACE` as the target like `-n`, and a template that names a
different namespace is refused under an exported env var.

**Why (simplicity).** A shell with `LEGION_NAMESPACE=dev` exported must still
be able to `legion job run prod-api.ncl` whose spec says `namespace =
"prod"`; kubectl's context namespace behaves this way. An explicit `-n` is
the user's statement about this command, and a disagreement with the spec has
no right winner.

---

## 25. KV stays path-scoped; namespaces do not own a KV subtree

**Revised** (2026-10-18). The call is unchanged, and now has evidence from
both incumbents.

**Call.** KV keeps its flat path space and the `paths` selector. A namespace
never becomes an implicit prefix on KV reads or writes.

**Alternatives.**

- (a) Namespace the KV tree the way Kubernetes namespaces Secrets.
- (b) Bind each namespace to a `<name>/` prefix and rewrite keys
  transparently.

**Trade-off.** A tenant has to be *told* its KV prefix, and nothing enforces
the link between namespace `payments-prod` and path `payments/`. Sharing
configuration across tenants stays easy, which is a feature or a hole
depending on who is asking.

**Why (simplicity, and the field wants sharing).** Both incumbents namespaced
their config stores, and users have been asking to share across that line
ever since:

- Kubernetes' ClusterSecret request is one of the most-upvoted namespace
  asks ([#70147](https://github.com/kubernetes/kubernetes/issues/70147), 211 👍,
  closed unbuilt), and copy-controllers like reflector (1.7k★) exist to work
  around it.
- Nomad variables can't be shared across namespaces at all
  ([#28455](https://github.com/hashicorp/nomad/issues/28455)).

Legion's KV already has a working, tested scoping mechanism. A second one
over the same data would put a "which mechanism applies" preamble on every KV
question.

---

## 26. Qualified references are `namespace/name`, with no new job-name validation

**Revised** (2026-10-18). The earlier version added job-name validation on
the write path and grandfathered existing names that contain `/`. Recon
found that `validate_object_name` already allows only `[A-Za-z0-9._-]`
(`src/structs/job.rs:1498`). So `/` was never legal, and nothing needs
grandfathering.

**Call.** `payments-prod/web` is accepted anywhere a job is named: in the
CLI, MCP, and the API's name fields, through one shared parser. It is the
display form in `-A` listings, the UI and structured output. A qualified
name that disagrees with `-n` or the `namespace` field is an error.

**Alternatives.**

- (a) `web@payments-prod`, Nomad-flavored.
- (b) `-n` only, with no qualified form.

**Trade-off.** Two ways to say the same thing, and a conflict rule between
them.

**Why (ease).** `ns/name` is the form Kubernetes output uses. It sorts by
tenant, and it matches the `/`-separated idiom Legion already uses for KV.
(b) would mean neither an agent nor an alert could emit one copy-pasteable
identifier. "Print something a human can paste back" is a stated
agent-readiness goal, and accepting the qualified form in the API's own name
fields makes it true for gRPC callers too.

---

## 27. No persisted CLI namespace context; `-n` exists only on commands that act on namespaced things

**Call.** The active namespace is `-n`, then `LEGION_NAMESPACE`, then
`default`. There is no `legion namespace use` and no namespace stored in
`~/.legion`. `-n` is attached per command by `namespace_flag()`, so on a
cluster-scoped command it is a clap error:

```console
$ legion node list -n prod
error: unexpected argument '-n' found
```

Every mutating command prints the qualified id it touched.

**Alternatives.**

- (a) kubectl's model: a namespace stored in the active context, set by
  `kubectl config set-context --current --namespace` or kubens.
- (b) Accept `-n` everywhere and ignore it where it doesn't apply.

**Trade-off.** Switching namespaces means typing `export LEGION_NAMESPACE=…`
in each shell, and there is no "remember this for me".

**Why (blast radius).** kubens has 20.0k★, so people want quick switching.
kube-ps1 has 3.8k★, so people need a prompt plugin to see where the switch
left them. The 2i2c outage was a production namespace deleted from the wrong
context with "no contextual clues"
([incident report](https://2i2c.org/incident-reports/2025-08-12-leap-outage)).
An env var dies with the shell, shows up in `env`, and never quietly follows
you into a new terminal. (b) is kubectl's `get pv -n x | delete` footgun
([kubectl#1413](https://github.com/kubernetes/kubectl/issues/1413)), where a
flag that looks like a scope is silently ignored.

---

## 28. Namespace metrics are new `legion_namespace_*` families labeled by namespace only; preemptions gain a label

**Implementation call** (2026-10-19). Plan § 8.9 asked for a `namespace`
label on every workload family.

**Call.** New gauges `legion_namespace_jobs`, `_allocations`,
`_reserved_cpu_mhz`, `_reserved_memory_bytes` and a `legion_job_submits_total`
counter, each labeled `{namespace}`; `legion_preemptions_total` gains
`namespace`. `legion_allocations` stays an unlabeled cluster total. There is
no `band` label on the reserved gauges.

**Alternatives.** (a) Relabel existing families (`legion_allocations{namespace}`).
(b) Label the reserved gauges by `{namespace, band}`, matching the quota.

**Trade-off.** Per-band usage is in `legion namespace get`, not in Prometheus;
families that do not exist (placements) gained no label.

**Why (performance).** Families cap at 256 label combinations. A
`{namespace, band}` gauge folds into `"other"` past about 40 namespaces; a
`{namespace}` one lasts to 256. Keeping `legion_allocations` unlabeled keeps
every existing dashboard correct.

---

## 29. Unnamed sessions are not charged against quota

**Implementation call** (2026-10-19).

**Call.** An unnamed session writes nothing to the replicated store until
promoted, so it is neither checked at creation nor charged. Named sessions
and every other instance are.

**Alternatives.** (a) A quota pre-check at session create against live usage.

**Trade-off.** A namespace can exceed its quota with unnamed sessions; the
public doc says so.

**Why (performance).** The session path is built to start without touching
the store; a pre-check there would read the cluster's jobs and allocations
on every session start, and still charge nothing afterwards, so it would
bound nothing.

---

## 30. Rollout, volume, snapshot and instance replies carry the canonical `job_id`, not separate `namespace` and `job_name` fields

**Implementation call** (2026-10-19). Plan § 11 gave every reply with a
`job_id` both fields.

**Call.** `Job`, `ServiceSummary`, `ServiceEndpoint`, `SchedulerEvent` and
`ServiceEdge.target` carry the namespace; the other replies carry only the
canonical `job_id`.

**Alternatives.** (a) Add `namespace` and `job_name` to every reply.

**Trade-off.** A client splits the id at the first `/` (bare means
`default`) for those replies.

**Why (simplicity).** The id is already canonical and total to split; the
fields would be derived data on a dozen messages, each one more place for the
two to disagree.
