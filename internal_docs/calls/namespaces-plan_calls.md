# Judgement Calls: Namespaces

Plan: [`plans/namespaces-plan.md`](../plans/namespaces-plan.md).

**Entries are ranked by project impact, highest first.** Impact means how
much of the system the decision constrains, how expensive it would be to
reverse, and how visible it is to users. The entry number is its rank.

These are the calls made while *writing* the plan. Nothing is implemented
yet. Anything resolved during implementation, and every deviation from the
plan, gets inserted here at its rank.

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
- (b) would have kept rename possible. This call closes it (call 10).

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
`namespaces = ["*"]` becomes boilerplate there. That cuts against call 14
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

## 4. Services are namespaced in the same phase as jobs, and crossing is explicit at both ends

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

## 5. `{namespace}` joins the workload-identity uniqueness floor and the default subject, with no pin-the-old-template option

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

## 6. Governance lives in namespace policies that select namespaces by glob and cap them in aggregate; priority bands stay in ACL rules

**Reversed in part** (2026-10-18). The earlier version put quota,
`allowedPools` and `allowedRuntimes` as fields **on the namespace object**.
The research broke that twice:

1. **Delegated creation dodges a per-namespace quota.** If `payments-*`
   creation is delegated (call 7), a quota on `payments-prod` is escaped by
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
[preemption call 5](preemption-and-eviction-plan_calls.md) rejected.

---

## 7. Namespace CRUD is a delegable ACL resource, and a cascade delete also needs `job:write`

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

## 8. Deletion refuses while non-empty; `--cascade` is synchronous and its final step is an atomic "delete if empty"; there is no `Terminating` state

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

## 9. A namespace conflict between the jobspec and the request is a server-side error

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

## 10. No namespace rename and no job move, decided up front; name reuse after deletion is allowed but made visible

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

## 11. No migration for pre-namespace data directories: the store refuses to upgrade and says so

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

## 12. Job templates stay cluster-scoped submit channels; running one lands the job in a target namespace

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

## 13. The UI puts the namespace in detail-route paths and the switcher in the page header, with no legacy redirects

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

## 14. Single-namespace identities never see tenancy UI at all

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

## 15. KV stays path-scoped; namespaces do not own a KV subtree

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

## 16. Qualified references are `namespace/name`, with no new job-name validation

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

## 17. No persisted CLI namespace context; `-n` exists only on commands that act on namespaced things

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
