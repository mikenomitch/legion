# Plan: Namespaces and Namespace-Scoped ACLs

> **Judgement calls:** [`calls/namespaces-plan_calls.md`](../calls/namespaces-plan_calls.md)

**Status: PLANNED.** Nothing here is built. The ACL rule vocabulary already
contains a `namespaces` selector and the scope struct already carries a
`namespace` field — both hardcoded to the constant `"default"`
(`src/structs/acl.rs:221`). This plan turns that placeholder into a real
tenancy boundary, and says explicitly which parts of "namespace" from
Kubernetes and Nomad Legion is **not** copying.

Deliberately deferred to named later phases (each restated in § Non-goals or
§ Phases): resource quotas, per-namespace node-pool and runtime restrictions,
namespaced KV, hierarchical namespaces, cross-namespace service intentions,
and namespace replication across federated regions.

---

## 1. Problem statement

Legion has no tenancy boundary. Every job, service, volume, template, KV key,
saved query, boot ticket and idempotency key lives in one flat global name
space, and the only tool for carving it up is a naming convention enforced by
ACL globs.

Concretely, here is what a platform team has to do today to give two teams a
shared cluster:

```bash
# Tell both teams, in a wiki page, to prefix every job name with their team.
legion acl policy apply team-a --rules '[
  {"resources": ["job"],    "actions": ["read","write"], "names": ["team-a-*"]},
  {"resources": ["exec"],   "actions": ["write"],        "names": ["team-a-*"]},
  {"resources": ["action"], "actions": ["write"],        "names": ["team-a-*"]},
  {"resources": ["scale"],  "actions": ["read","write"], "names": ["team-a-*"]},
  {"resources": ["kv"],     "actions": ["read","write"], "paths": ["team-a/*"]}
]'
# ...and again for team-b, and again for every new resource type Legion grows.
```

What breaks:

- **The prefix is the tenancy boundary, and it is invisible to the system.**
  `legion job list` shows both teams interleaved. Nothing tells an operator
  which jobs belong to whom except a substring convention that Legion never
  validates, never displays, and never groups by.
- **Every new resource needs a new selector line in every tenant policy.**
  `scale`, `boot`, `template`, `exec`, `action` each arrived with their own
  selector. A policy written before `scale` existed silently failed open on
  the day `scale` shipped — nothing named the *tenant*, so nothing could be
  extended to cover it.
- **Name collisions are the tenant's problem.** Two teams cannot both run a
  job called `web`. They rename to `team-a-web` and `team-b-web`, and then
  every dashboard, alert and runbook carries the prefix forever.
- **There is nowhere to hang tenant-wide governance.** A quota, a default
  priority band, an allowed node-pool list, a "this tenant may not use the
  `exec` runtime" rule — each of these has no object to attach to, so each
  would arrive as its own config map keyed by a string that means "team".
  We already rejected one of those (`priority.maxBandByNamespace`, see
  [preemption plan open question 5](preemption-and-eviction-plan.md)) on the
  grounds that the namespace would be the right anchor. That anchor does not
  exist yet.
- **Several global name spaces are cross-tenant leaks waiting to happen.**
  The idempotency table is keyed by the caller-supplied key alone
  (`src/storage/store.rs:16649`): team B replaying `deploy-1` gets team A's
  recorded reply. Service names are global, so the second team to advertise
  `web` competes for the same catalog entry the first team's `connect` block
  resolves. Neither is a namespace bug today only because there are no
  tenants.

And the sharpest one:

- **Namespace-scoped policies silently grant nothing.** An admin who reads
  the ACL docs, sees `namespaces` in the selector list, and writes
  `{"resources": ["job"], "actions": ["*"], "namespaces": ["prod"]}` gets a
  policy that authorizes **nothing at all**, with no validation error and no
  warning. Every scope in the product is stamped `"default"`
  (`src/structs/acl.rs:238`), and a selector that names a value the request
  never carries fails closed. This is documented behavior
  (`docs/auth-and-acls.md`: "jobs live in `default` until namespaces land")
  but it is exactly the shape of footgun that gets discovered at 3am.

---

## 2. What exists today, and what is wrong with it

### 2.1 The existing "namespace implementation"

Legion's namespace support today is **vocabulary without values**. The pieces:

| Piece | Location | State |
| --- | --- | --- |
| `namespaces` rule selector | `src/structs/acl.rs:122` | Real, glob-matched, unit-tested |
| `AclScope.namespace` | `src/structs/acl.rs:194` | Real field |
| `DEFAULT_NAMESPACE = "default"` | `src/structs/acl.rs:221` | The only value ever produced |
| `AclScope::job/exec/action/job_via_template` | `src/structs/acl.rs:238-301` | All hardcode `DEFAULT_NAMESPACE` |
| UI mirror `DEFAULT_NAMESPACE` | `ui/src/utils/acl.ts:59` | Same constant, same hardcode |
| Audit log `namespace` field | `src/manager/acl.rs:633` | Always `default` or `-` |
| Net-policy `NetSelector.namespaces` | `src/structs/netpolicy.rs:106` | Real glob list, matched against a fact that is always `default` |
| Jobspec `namespace` field | — | Does not exist |
| `Namespace` object / RPC / CLI | — | Does not exist |

**Downsides of this as it stands:**

1. **Silent no-op policies** (see § 1). A `namespaces` selector naming
   anything but `default` denies everything, quietly. There is no validation
   at `legion acl policy apply` that the namespace exists, because namespaces
   do not exist.
2. **Indistinguishable from having no selector.** Because every job scope is
   stamped `default`, a rule with `namespaces: ["default"]` and a rule with
   no `namespaces` at all behave identically on every endpoint. Nobody can
   tell from behavior which endpoints are namespace-aware — the answer is
   "none of them, and all of them".
3. **The upgrade hazard is already baked in.** Every policy written to date
   omits `namespaces`, and an omitted selector means *any*. The day the
   second namespace is created, every existing policy applies to it. A
   `names: ["web-*"]` policy becomes "web-* in every namespace, forever,
   including namespaces created next year." Any design here has to answer
   this explicitly (§ 7.4).
4. **Documentation promises are running ahead of the code.** The preemption
   work shipped the `bands` ACL selector and
   [TODO.md](../TODO.md) records "delivers per-namespace band caps" as done.
   Half of it is: the selector composes with `namespaces`, but there is no
   namespace to scope to, so "per-namespace band caps" is today just "band
   caps". The public example in `docs/auth-and-acls.md` uses
   `namespaces: ["prod"]` in a policy that, pasted into a live cluster,
   grants nothing.
5. **Downstream designs are blocked on it.** The federation plan calls
   namespaces "vocabulary-complete but value-empty" and defers on them
   ([multi-cluster-federation-plan.md § 16.5](multi-cluster-federation-plan.md)).
   Workload identity's default subject template
   (`src/structs/workload_identity.rs:90`) is documented as "Nomad's composed
   subject minus the namespace segment Legion lacks".

### 2.2 The code the design has to fit

- **Job identity is a global name.** `store.get_job(name)`
  (`src/storage/store.rs:7997`) is a single-column lookup. The `job` table has
  `id` as its primary key with a `name` column
  (`src/storage/migrations.rs:37`); uniqueness of `name` is enforced by the
  submit path's existence check (`src/manager/server.rs:1608`), not by an
  index. `job_config`, `task_config`, `allocation`, `eval`, `job_update` all
  hang off `job_id`, so **the foreign-key graph does not need to change** —
  only the way a name is resolved to an id.
- **Job names are barely validated.** Volume names and template names have
  validators (`src/structs/job.rs:1435`, `src/manager/job_templates.rs:52`);
  job names have a length check and nothing else. Any qualified-reference
  syntax has to reckon with that (§ 7.2).
- **ACL enforcement is centralized and already scope-shaped.** 82 `check_acl`
  call sites across 119 manager RPCs, 25 of which build an `AclScope::job(...)`
  (`src/manager/server.rs`). Adding the namespace is a mechanical change at
  each site plus a compile-time push: making `AclScope::job` take a namespace
  turns every missed site into a build error.
- **List endpoints already filter per item.** `list_jobs`
  (`src/manager/server.rs:3758`) gates on `AclScope::any()` then `retain`s on
  `identity.allows(...)`. Namespace filtering rides the same path.
- **`tests/integration/acl_endpoints.rs` is a hard gate.** `every_manager_rpc_is_classified`
  parses the proto and fails the build for an unclassified RPC, so new
  namespace RPCs cannot ship unprotected.
- **Events are all-or-nothing.** `list_events` / `stream_events` gate on
  `operator:read` with `AclScope::none()` (`src/manager/server.rs:3250`) and do
  no per-item filtering. A namespace tenant either sees every event in the
  cluster or none. This is a real gap the design has to close (Phase 3).
- **Idempotency is globally keyed** (`src/storage/store.rs:16649`).
- **Services are a projection of allocations**, keyed by service name
  (`docs/service-discovery.md`); `connect` upstreams and `allow` intentions
  reference bare service names.
- **The UI keys routes on job name** (`ui/src/App.tsx`, `detailRoutes`), and
  mirrors ACL evaluation client-side (`ui/src/utils/acl.ts`) for cosmetic
  gating.
- **The CLI is clap's builder API** (`src/main.rs`), with shared helpers
  (`json_flag`, address/token resolution) in `src/cmd/helpers.rs`. There is no
  global-flag mechanism today; `--json` is added per subcommand. `LEGION_ADDR`
  and `LEGION_TOKEN` set the precedent for env-var defaults.

---

## 3. Prior art

### 3.1 Kubernetes

Namespaces are cluster-scoped objects; most resources are namespaced, some
(Nodes, PersistentVolumes, StorageClasses, ClusterRoles, CRDs) are not. Names
are unique per namespace. DNS encodes it (`svc.ns.svc.cluster.local`). RBAC
splits into namespaced `Role`/`RoleBinding` and cluster-wide
`ClusterRole`/`ClusterRoleBinding`. `ResourceQuota` and `LimitRange` attach per
namespace. Deleting a namespace cascades to its contents.

**What goes wrong, in the field:**

- **"A namespace is not a security boundary" is the single most repeated
  correction in the K8s ecosystem.** Without NetworkPolicy there is no network
  isolation; without a separate node pool there is no compute isolation; the
  kernel is shared. Users read "namespace" as "tenant" and get neither.
- **Terminating-forever namespaces.** Cascading deletion runs through
  finalizers; one broken finalizer or unreachable aggregated APIService wedges
  the namespace in `Terminating` indefinitely, and the documented fix is
  hand-editing the object's finalizer list. It is one of the most-searched K8s
  failure modes in existence.
- **Two-level RBAC is a permanent tax.** "Do I need a Role or a ClusterRole"
  is a question every K8s user gets wrong at least once, and a ClusterRole
  bound in a namespace behaves differently from the same ClusterRole bound
  cluster-wide.
- **Quotas are brittle.** `ResourceQuota` only works if every pod sets
  requests, so you need `LimitRange` to backfill defaults, and turning a quota
  on in a live namespace breaks existing workloads that never set requests.
- **Flat namespaces do not model organizations.** The Hierarchical Namespace
  Controller, Capsule, vcluster and the entire multi-tenancy SIG exist because
  one flat level is not enough — and because the level that *is* there is not
  isolating.
- **Cross-namespace references are inconsistently allowed**, which is why
  Gateway API had to invent `ReferenceGrant`.
- **No rename, and `default` is a trap** — everything lands there because the
  tool defaults to it, and there is no path off it except recreate.

**Take:** namespaced identity, per-namespace name uniqueness, and one place to
hang policy. **Reject:** cascading deletion with finalizers, the two-tier
role model, and any suggestion that a namespace isolates compute or network.

### 3.2 Nomad

Namespaces are first-class objects (`nomad namespace apply`), open-sourced in
Nomad 1.0 after years as an Enterprise feature. Jobs, allocations, evals,
deployments, CSI volumes, variables, services and scaling policies are
namespaced; nodes and node pools are not. ACL policies contain
`namespace "dev-*" { capabilities = [...] }` blocks with glob matching.
Namespace objects carry quota (Enterprise), node-pool configuration
(default/allowed/denied, Enterprise), enabled task drivers, and metadata.
`-namespace` flag, `NOMAD_NAMESPACE` env var, `-namespace '*'` for
cross-namespace listing. Deleting a non-empty namespace is refused.

**What goes wrong:**

- **OSS namespaces were "just a prefix with ACLs"** for a long time: the
  governance that makes a namespace worth having (quota, node-pool
  restrictions) was Enterprise-gated, so the open-source experience was
  naming plus authorization and nothing else.
- **Namespaces did not give compute isolation, so node pools were added in
  1.6.** Before that, tenants used constraints and datacenters. This is the
  clearest available evidence that namespace ≠ isolation, from the codebase
  Legion most resembles.
- **"Job not found" from the wrong namespace is the #1 support question.**
  Every command needs the namespace; the default is invisible until it bites.
- **Consul integration doubles the concept.** Nomad namespaces and Consul
  namespaces are different objects that have to be mapped to each other, and
  the mapping is a config field people get wrong.
- **`*` as a namespace is a per-command flag,** so scripts silently see one
  namespace unless they remember it.

**Take:** the whole shape — first-class object, glob-matched ACL scoping,
empty-before-delete, env var + flag, `*` for cross-namespace reads.
**Reject:** gating the governance half behind a licence, and letting the
namespace object accumulate a second policy vocabulary that overlaps ACLs.

### 3.3 Others, briefly

- **Consul** needed *admin partitions* on top of namespaces, because
  namespaces alone did not isolate the catalog or the mesh — a second warning
  that a naming boundary gets asked to do isolation work it cannot do.
- **Vault** namespaces are hierarchical path segments (`ops/team-a/`). Elegant
  for delegation; the cost is that token and policy resolution across
  namespaces is genuinely hard to reason about.
- **Borg** has no namespace object at all: a job is `<cell>/<user>/<name>`,
  and the *user* (with quota assigned per user, per priority, per cell) is the
  tenancy unit. Tenancy is attached to the identity that submits, not to a
  container object.
- **Docker Swarm** has stacks — a label (`com.docker.stack.namespace`) and a
  name prefix. Exactly Legion's status quo, made official.
- **ECS** has no namespaces: the cluster is the boundary, and scoping is IAM
  plus tags.

---

## 4. Jobs to be done, by persona

**Security / platform admin**

- When I hand a team access to the shared cluster, I want to give them one
  name that means "their stuff", so I can write a permission rule once
  instead of maintaining a list of prefixes that grows every time Legion
  grows a feature.
- When I add a new team, I want to be sure their permissions do not
  accidentally reach into another team's work, so I do not have to audit
  every existing rule to find out who just got access.
- When someone leaves the company and I revoke their access, I want to see
  exactly what they could reach, so I can answer "what was exposed" honestly.

**Cluster operator**

- When I look at a cluster that several teams share, I want to see whose
  work is whose at a glance, so I can tell which team to call when something
  is wedged.
- When a team is finished with an environment, I want to remove it and
  everything in it in one step, without leaving orphans behind and without
  the removal getting stuck half-done.
- When one team is filling the cluster, I want a way to cap what they can
  take, so one team's bad afternoon is not everyone's bad afternoon.

**Job author / app developer**

- When I name my service, I want to call it what it is — `web`, `api`,
  `worker` — without checking whether another team already took the name.
- When I look at the cluster, I want to see my team's work by default and not
  have to filter through everyone else's.
- When I copy a deployment from staging to production, I want the only
  difference to be where it lands, not a renamed job and a rewritten set of
  dashboards.

**On-call responder**

- When I am paged about a service, I want the thing I am looking at to say
  which team owns it, so I know whether I can restart it or need to wake
  someone.
- When I have permission for one team's environment and not another's, I want
  the tools to show me that clearly, rather than failing on the fifth command
  I type.

**Autonomous agent**

- When I am given credentials for one team, I want every list I read and
  every action I take to stay inside that team without my having to remember
  a flag, so I cannot damage anything outside my remit even if I misparse a
  name.
- When I report what I did, I want the identifier I print to be unambiguous
  cluster-wide, so a human reading my output can find the same thing.

### User scenarios the design must serve

1. **Two teams, same job name.** Team A runs `web`, team B runs `web`, both in
   the same cluster. Neither can see, stop, exec into, scale or resolve the
   other's. Both appear in one operator view, distinguished by namespace.
2. **A tenant token that cannot escape.** A token scoped to `team-a` lists
   jobs, reads events, resolves services, opens exec sessions — every one of
   them filtered to `team-a`, with no flag involved and no way to opt out.
3. **An operator retiring an environment.** `legion namespace delete
   staging-old` refuses because it is not empty, says what is in it, and
   `--cascade` drains it in one command with no wedged half-deleted state.

---

## 5. Goals and non-goals

### Goals

1. A first-class `Namespace` object with CRUD, over gRPC, CLI, MCP and UI.
2. Jobs (and the surfaces that hang off a job: instances, exec, actions,
   scale, boot tickets, rollouts, events) carry a namespace, and job identity
   becomes `(namespace, name)`.
3. ACL rules scope by namespace using the **existing** `namespaces` selector,
   with no new policy vocabulary and no second policy engine.
4. Namespace-aware defaults everywhere a human types: `-n/--namespace`,
   `LEGION_NAMESPACE`, `--all-namespaces`, a UI switcher, and qualified
   `namespace/name` references accepted anywhere a job name is accepted.
5. Service discovery is namespace-isolated: two namespaces' `web` services do
   not collide and do not cross-resolve.
6. Every global name space that would leak across tenants is closed:
   idempotency keys, service names, and (in later phases) templates and
   volumes.
7. A migration in which **no existing job, policy, script or bookmark
   breaks**, and in which the one behavioral change lands once and never
   again.

### Non-goals

- **Namespaces are not an isolation boundary.** No network isolation, no
  kernel isolation, no implicit node separation. A namespace is a *naming,
  authorization and governance* boundary. Compute isolation is what node pools
  are for, network isolation is what `network.mode = "gated"` and net policies
  are for ([docs/network-handlers.md](../../docs/network-handlers.md)), and
  stronger isolation is what federation is for. Creating a namespace changes
  none of them, and a namespace never implies a network mode — a tenancy
  boundary that silently rewires a workload's networking is a tenancy boundary
  that breaks deploys. This will be stated in the public doc, in the CLI help,
  and in the proto comment, because every prior system that left it implicit
  had users assume the opposite.
- **No hierarchical namespaces.** `team-a/prod` as a *nested object* is out.
  Glob matching on flat names (`namespaces: ["team-a-*"]`) covers the
  delegation cases we can actually name today, and Vault's hierarchy is the
  standing warning about what the resolution rules cost.
- **No per-namespace RBAC objects.** No namespaced policies, no
  `Role`/`ClusterRole` split. Policies stay cluster-scoped documents that
  *select* namespaces.
- **No namespace rename.** Same as K8s and Nomad. Recreate and resubmit.
- **No cascading-delete finalizer machinery.** Deletion is refused while
  non-empty, or `--cascade` does the work synchronously and reports what it
  did. There is no `Terminating` state, ever.
- **No namespaced KV in v1.** KV already has a working path-prefix scoping
  mechanism (`paths` selectors). A second scoping mechanism over the same
  data is the "two concepts where one would do" failure. Revisit only if a
  concrete request arrives (§ 14).
- **No quota in the first phases.** It is Phase 5, behind an explicit design
  decision about where enforcement happens.
- **Not a scheduling input.** The scheduler does not learn about namespaces
  in any phase of this plan; placement is unchanged and unmeasured because
  it is untouched.

---

## 6. Alternative implementations, compared

Six shapes were genuinely on the table. The comparison is what the design in
§ 7 is arguing against.

| # | Shape | Identity | Migration cost | Delivers collision-free names | Delivers policy anchor | Delivers governance anchor |
| --- | --- | --- | --- | --- | --- | --- |
| A | Naming convention only (status quo) | global name | none | no | partial (via globs) | no |
| B | Labels + label selectors in ACLs | global name | low | no | yes, N-dimensional | weak |
| C | Namespace as an attribute; names stay globally unique | global name | low | **no** | yes | yes |
| D | Namespace as a required name prefix (`team-a/web` is the name) | global name | medium | yes | yes | yes |
| E | **Namespace as an object; identity is `(namespace, name)`** | compound | high | **yes** | **yes** | **yes** |
| F | One cluster per tenant (federation) | n/a | very high | yes | yes | yes |

**A — convention only.** Free, and it delivers more than people assume: a
`names: ["team-a-*"]` rule genuinely prevents team A from creating
`team-b-web`. What it cannot do is show the tenancy anywhere, survive the
arrival of a new resource type, or give governance somewhere to live. It is
where we are, and § 1 is the list of what it costs.

**B — labels.** Attractive because it is N-dimensional: `team`, `env`, `tier`
all at once, and one job can be in several groupings. Rejected on a security
argument: the label is **self-asserted by the submitter**. Making a mutable,
submitter-controlled attribute the thing an authorization rule matches on
means the rule is only as strong as the admission control that binds labels
to identities — which is a second system we would have to build to make the
first one safe. Kubernetes has had labels since day one and has never let
RBAC select on them, for exactly this reason. Labels remain a good idea for
*organization* and are orthogonal to this plan.

**C — namespace as an attribute, names still globally unique.** Cheap: add a
column, stamp it on the scope, filter lists. It delivers the policy anchor,
the governance anchor and the UI grouping — everything except collision-free
names. Tempting to ship and stop. Rejected as an endpoint, but **adopted as
the first phase**, because as a phase it is the entire feature minus one
constraint, and getting there first means the remaining work is a *relaxation*
(see § 7.1 and call 1).

**D — namespace as a name prefix.** The namespace object exists for policy and
governance, but identity stays a single string that happens to start with
`team-a/`. No compound keys, no dual-column indexes, trivially compatible with
every existing lookup. Rejected because the namespace and the name are then
not independent: you cannot move a job between namespaces, a namespace rename
rewrites every job identity, and every consumer has to parse the string to
recover the tenant — which means the parse rule becomes load-bearing in the
UI, the CLI, the event stream and every dashboard. It buys a smaller diff and
pays with an ambiguity that never goes away. Docker Swarm stacks are this
design, and "the stack is a label on the name" is the thing people complain
about.

**F — one cluster per tenant.** The only option that actually isolates.
Already planned as a separate feature
([multi-cluster-federation-plan.md](multi-cluster-federation-plan.md)), whose
own design assumes namespaces exist and replicates them in the meta-plane.
Not an alternative to namespaces; a complement, at a different price point
(one control plane per tenant).

**Chosen: E, delivered through C.** Namespace is a first-class object; job
identity becomes `(namespace, name)`; the path there starts by shipping the
attribute and the scoping with names still globally unique, then relaxes the
uniqueness constraint. See § 12 for why that ordering makes the migration
free.

---

## 7. Proposed design

### 7.1 The namespace object

A namespace is a small, mostly-empty object. It exists to be *referred to*.

```nickel
# namespaces/prod.ncl
{
  name = "prod",
  description = "Production workloads",
  meta = { owner = "platform", slack = "#prod-alerts" },
}
```

```bash
legion namespace list                      # NAME | JOBS | DESCRIPTION | CREATED
legion namespace create prod --description "Production workloads"
legion namespace apply prod -f namespaces/prod.ncl
legion namespace get prod -j
legion namespace delete staging-old
legion namespace delete staging-old --cascade
```

Rules:

- **Name grammar** is the template-name grammar (`src/manager/job_templates.rs:52`):
  lowercase letters, digits and dashes, starting and ending alphanumeric, ≤ 64
  chars. `*` and `all` are reserved (they are the wildcard in every read
  surface); `default` always exists and cannot be deleted.
- **Creation is not implicit.** Submitting a job to a namespace that does not
  exist fails with a message naming the missing namespace. Kubernetes and
  Nomad both refuse too, and implicit creation would mean a typo silently
  forks a tenant.
- **Deletion refuses while non-empty**, listing what is in the way:

  ```console
  $ legion namespace delete staging-old
  error: namespace 'staging-old' is not empty: 3 jobs, 1 volume
  hint: `legion job list -n staging-old` to see them, or re-run with --cascade
  ```

  `--cascade` stops and destroys the contents synchronously and reports each
  step. There is no `Terminating` state and no finalizer: if the cascade
  fails halfway, the namespace is still there, still deletable, and the
  operator can see exactly what remains. This is the single most important
  thing we are *not* copying from Kubernetes.
- **No rename** (§ 5).

### 7.2 Identity and references

Job identity becomes `(namespace, name)`. Everywhere a job is named, three
forms are accepted:

| Form | Meaning |
| --- | --- |
| `web` | `web` in the **active namespace** |
| `prod/web` | `web` in `prod`, regardless of the active namespace |
| `-n prod web` | same |

The active namespace resolves as: `--namespace/-n` flag → `LEGION_NAMESPACE`
env var → `default`. Same precedence shape as `LEGION_ADDR`/`LEGION_TOKEN`
(`src/cmd/helpers.rs:107`), so there is nothing new to learn.

Qualified `namespace/name` is the display form too: `legion job list -A` prints
`prod/web`, the UI shows `prod / web`, events and logs carry both fields. An
agent printing `prod/web` has printed something a human can paste back.

**Job names must not contain `/` from Phase 1 onward.** Job names are barely
validated today (§ 2.2), so `a/b` is currently a legal — if unheard-of — job
name, and qualified references would be ambiguous. Phase 1 adds job-name
validation on the *write* path only (same grammar as namespaces, plus dots and
underscores for compatibility with names already in the wild), grandfathering
rows that already exist. A pre-existing job with a slash in its name stays
addressable with `-n`; the error message says so.

**Conflict between the flag and the jobspec is an error, not a precedence
rule:**

```console
$ legion job run api.ncl -n dev      # api.ncl declares namespace = "prod"
error: jobspec declares namespace 'prod' but -n specified 'dev'
hint: remove one; a jobspec that names its namespace does not need the flag
```

Nomad lets the flag win, kubectl errors. We error: silent precedence between
two things a human wrote in the same breath is how a staging deploy lands in
production.

**Not-found messages help, without leaking:**

```console
$ legion job status api
error: job 'api' not found in namespace 'dev'
hint: a job named 'api' exists in namespace 'prod'; retry with -n prod
```

The hint appears **only when the caller has `job:read` in the namespace it
names**. Otherwise the message is the bare not-found — a cross-namespace hint
shown to an unauthorized caller is an existence oracle, which is the exact
failure the "check before you fetch" rule in AGENTS.md exists to prevent.

### 7.3 The jobspec surface

```nickel
{
  name = "api",
  namespace = "prod",        # optional; defaults to the active namespace
  tasks = [ ],
}
```

One new optional string. It goes into `configs/jobs/maximal.ncl` with the
rest. Tasks get `$LEGION_NAMESPACE` in their environment alongside the job and
task names, so a workload can label its own metrics and logs without the
operator plumbing it through.

### 7.4 ACL model

**No new rule vocabulary.** The `namespaces` selector already exists, already
globs, and already composes with `names`, `bands`, `templates` and the rest.
What changes is that it starts matching something real.

```nickel
# team-a's policy, complete
[
  { resources = ["job", "exec", "action", "scale", "boot", "net"],
    actions = ["read", "write"], namespaces = ["team-a"] },
  { resources = ["namespace"], actions = ["read"], namespaces = ["team-a"] },
  { resources = ["kv"], actions = ["read", "write"], paths = ["team-a/*"] },
  { resources = ["node"], actions = ["read"] },
]
```

Compare that with the prefix-glob policy in § 1: one selector names the
tenant, and a resource type added next year is covered by adding it to the
`resources` list rather than by inventing another selector.

**One new resource, `namespace`:**

| Resource | `read` | `write` | Scoped by |
| --- | --- | --- | --- |
| `namespace` | list/get namespaces | create/update/delete | `namespaces` |

It is a separate resource rather than part of `operator` for the reason
`scale` and `boot` are separate: `operator` means raft, manager configs and
observability, and "may create a namespace" should not imply "may reconfigure
raft". Scoping by `namespaces` means "a team lead may create `team-a-*`
namespaces" is expressible on day one.

**Which resources become namespaced:**

| Namespaced | Cluster-scoped |
| --- | --- |
| `job`, `exec`, `action`, `scale`, `boot`, `net`¹, `template`² | `node`, `operator`, `state`, `acl`, `kv`³ |

¹ `net` already documents itself as scoped by `pools` / `namespaces`
(`src/structs/acl.rs:54-60`); it needs no rule-surface change, only real values.
² Templates become namespaced in Phase 4; until then they are cluster-scoped
and a `templates` selector means what it means today.
³ KV stays path-scoped (§ 5 non-goals).

**The omitted-selector hazard, answered.** A rule with no `namespaces`
selector matches **every** namespace, including ones created later. That is
consistent with every other selector in the engine, and making one selector
special ("omitted means `default`") would break the single rule the whole
model rests on. Instead the hazard is made *visible*, three ways:

1. `legion acl policy apply` warns at write time:

   ```console
   $ legion acl policy apply team-a --rules @team-a.ncl
   Applied policy 'team-a'.
   warning: rule 0 (job:read,write) has no `namespaces` selector — it applies to
            all 4 namespaces, including any created later.
   ```

2. `legion namespace create` reports the blast radius before it acts:

   ```console
   $ legion namespace create team-b --dry-run
   Would create namespace 'team-b'.
   3 existing policies would apply to it because they name no namespace:
     platform-admin (job:*, kv:*)   ops-readonly (job:read)   ci (job:write)
   ```

3. The upgrade note in `docs/upgrades.md` and the public page say it plainly.

**Enforcement mechanics.** `AclScope::job(name)` becomes
`AclScope::job(namespace, name)` — a signature change, so every one of the 25
call sites in `src/manager/server.rs` becomes a compile error until it supplies
a namespace. The same applies to `exec`, `action`, `job_via_template`. Nothing
can be forgotten silently. The coarse-then-precise pattern is unchanged: gate
with `AclScope::any()` before a lookup, enforce the precise
`(namespace, name)` scope after it.

**List filtering** already works per item (`src/manager/server.rs:3758`); with
the namespace stamped on the item's scope, a `team-a` token sees `team-a`'s
jobs and nothing else, with or without `-n`.

### 7.5 The subsystems that must not leak

These are the parts a "namespaces" feature usually forgets, and each one is a
tenant-visible bug if it is left out.

**Service discovery and the mesh (Phase 3).** A service inherits its job's
namespace. Catalog entries are keyed `(namespace, service)`. `connect`
upstreams resolve same-namespace by default; crossing requires naming it *and*
being allowed by the target's intentions (which are job ids, so they become
namespace-qualified along with jobs):

```nickel
service = { port = 8080, allow = ["gateway", "team-b/frontend"] },
connect = { upstreams = [
  { service = "redis" },                          # same namespace
  { service = "auth", namespace = "shared" },     # explicit crossing
] },
```

The mesh now gives services **DNS names** — `<service>.svc.legion`
([docs/service-discovery.md](../../docs/service-discovery.md),
[service-mesh-plan.md](service-mesh-plan.md)) — so namespacing the catalog has
to name the DNS shape too. Proposal, following Kubernetes because operators
already read it fluently: the fully qualified form gains a namespace label,
`<service>.<namespace>.svc.legion`, and the short form `<service>.svc.legion`
continues to resolve **within the requesting allocation's own namespace**. The
short form staying same-namespace is what keeps every existing jobspec correct
and makes the common case the un-typed one; the long form is what makes a
deliberate crossing visible in the config that does it.

Until this phase lands, **namespaces do not isolate discovery** — two
namespaces' `web` services share one catalog name and one DNS name. That
sentence goes in the public doc from Phase 1, not Phase 3.

Net policies (`NetSelector`, `src/structs/netpolicy.rs:106`) already carry a
`namespaces` glob list against a `NetSelectorFacts.namespace` that is always
`default`, so they inherit real values from Phase 1 with no change to the
policy surface at all — a selector an operator writes today starts scoping the
day namespaces ship. This is the second consumer waiting on this plan, and the
reason Phase 1 stamps the namespace onto the selector facts rather than only
onto ACL scopes.

**Workload identity (Phase 2, and it is a security dependency).** The default
subject template is `{region}:{job}:{group}:{task}`
(`src/structs/workload_identity.rs:90`). The moment two namespaces can both
contain a job called `web`, that template mints **identical subjects for
different tenants** — an IdP trust policy written for one team would accept
the other's token. So:

- `{namespace}` becomes an available placeholder in Phase 1.
- The cluster default template becomes
  `{region}:{namespace}:{job}:{group}:{task}` **in the same phase that relaxes
  name uniqueness** (Phase 2), never later.
- Because that changes every existing subject, it is a documented breaking
  change with an upgrade note (rewrite IdP trust policies), and clusters that
  cannot take it can pin the old template in config — with a startup warning
  once a second namespace exists, naming the risk.

**Idempotency keys (Phase 1).** The record table is keyed by the caller's key
alone (`src/storage/store.rs:16649`), so two tenants using `deploy-1` share a
record and the second one gets the first one's reply. The key becomes
`(namespace, key)`, with `-` for cluster-scoped operations. Migration:
existing rows land in `default`.

**Events (Phase 3).** `list_events`/`stream_events` gate on `operator:read`
and filter nothing (`src/manager/server.rs:3250`). Events gain a `namespace`
field, and the endpoints gain a second path: a caller with `job:read` in a
namespace may read that namespace's events, filtered per item like every other
list. `operator:read` continues to see everything, including cluster-scoped
events (node, raft, scheduler) which carry no namespace.

**Volumes and templates (Phase 4).** Both are globally named today. Volumes
follow their job's namespace; templates become namespaced objects with a
cluster-scoped `shared` namespace for platform-published templates. Both are
Phase 4 because neither is a leak until tenants exist, and both need their own
migration.

### 7.6 Governance on the namespace object (Phase 5)

Once the object exists, the governance requests that have been accumulating
have somewhere to live:

```nickel
{
  name = "team-a",
  quota = { cpu = 200000, memory = "512Gi", jobs = 200 },  # cluster-wide caps
  allowedPools = ["shared-*", "team-a-gpu"],              # where its jobs may land
  allowedRuntimes = ["container", "sandbox"],              # which task kinds it may use
}
```

Two of these overlap with things ACLs could express, and the split is
deliberate: **ACL rules answer "may this principal do X"; namespace governance
answers "may anything in this tenant do X"**. A quota that a principal could
dodge by using a different token is not a quota. Priority bands stay in ACLs
(where the preemption work put them) precisely because the interesting
question there *is* per-principal — "the autoscaler may claim `high`, humans
may not". This asymmetry is recorded as a judgement call rather than smoothed
over, because it is the kind of thing that looks like an inconsistency until
someone explains it.

Enforcement point for quota is an open question (§ 14).

---

## 8. Non-namespace ways to reach the same goals

The user-facing goals are: (a) permission scoping, (b) collision-free names,
(c) legibility of a shared cluster, (d) a governance anchor, (e) blast-radius
containment. Legion can already reach several of these without namespaces, and
this section is the honest accounting of what is actually left over.

| Mechanism | Status in Legion | Delivers | Does not deliver |
| --- | --- | --- | --- |
| Name prefixes + `names` globs | shipped | (a) mostly, (b) by convention | (c) (d) (e) |
| Node pools + `pools` selector | shipped | compute isolation | naming, permissions on jobs |
| Job templates as the submit channel | shipped | (a) strongly, (e) strongly | (b) (c) (d) |
| Priority bands + `bands` selector | shipped | contention governance | (a) (b) (c) |
| KV path prefixes | shipped | (a) for KV | everything else |
| Workload identity subjects | shipped | per-workload authz outward | nothing cluster-internal |
| Separate clusters + federation | planned | everything, strongly | cheapness |
| Labels + label-selector ACLs | not built | (a) N-dimensionally | (b), and it is unsafe (§ 6 B) |
| Per-token quota (Borg's model) | not built | (d) | (a) (b) (c) |

Three of these deserve more than a table row, because each is a genuine
alternative that a reasonable engineer would propose instead of this plan.

**Job templates are already a stronger boundary than namespaces for the
"guardrails" job.** A team restricted to `templates: ['render']` cannot submit
an arbitrary spec at all — that is admission control, and it is strictly
stronger than "you may submit anything you like as long as it is named
`team-a-*`". If the actual requirement is "this team may only run these
workloads", namespaces are the wrong tool and templates are the right one,
today, with no new code. The plan does not replace that; it complements it.

**Node pools already solve the isolation half.** If the requirement is "team
A's work must not share machines with team B's", that is a pool, and it works
now. Every system surveyed in § 3 eventually had to build this *separately
from* namespaces (Nomad in 1.6, Consul admin partitions, K8s node pools +
NetworkPolicy). Legion having it first is why this plan can say "namespaces
are not an isolation boundary" without leaving users stranded.

**Per-token quota — the Borg model — is a real alternative to the governance
half.** Attach quota to the identity that submits rather than to a container
object, and you get (d) with no new noun at all. It was rejected because
quota-per-token means a team with three CI tokens has three quotas, and
because it gives (c) and (b) nothing. But it is worth naming: the *only*
reason the governance anchor has to be a namespace is that we also want (b)
and (c), and those force a shared object anyway.

**What is genuinely left that only namespaces deliver:**

1. **Collision-free names** — no composition of the shipped mechanisms gives
   two teams a job called `web`.
2. **One anchor for tenant-wide facts** — quota, allowed pools, allowed
   runtimes, ownership metadata. Without it, each arrives as its own config
   map keyed by a string that means "team", which is a namespace with worse
   ergonomics and no CRUD.
3. **Legibility** — a column, a filter, a switcher, a grouping. Convention
   cannot be rendered.
4. **A rule that survives Legion growing** — `namespaces: ['team-a']` covers
   resources that do not exist yet; `names: ['team-a-*']` covers the resources
   whose selectors someone remembered to add.
5. **Recognition** — "namespace" is the word every operator arriving from
   Kubernetes or Nomad already has. That is not a technical argument, and it
   is not nothing.

That list is the justification for the whole plan. If items 1–5 are not worth
the cost in § 12, the right answer is to write down the convention, improve
the `names`-glob ergonomics, and stop — and this section is deliberately
written so that decision stays available to whoever reads it next.

---

## 9. UI implementations, compared

The UI is where a tenancy model is either legible or useless, and it is where
the K8s and Nomad experiences differ most. Five shapes, then the pick.

**Option 1 — Sidebar/topbar switcher, state in localStorage.**
A dropdown pinned in the app shell; everything below it is filtered.
*Used by:* Kubernetes Dashboard, Nomad's namespace dropdown, most SaaS org
switchers.
*Pros:* one obvious control; matches the mental model "I am working in a
namespace"; every page inherits it for free.
*Cons:* if the selection lives only in browser storage, **links are not
shareable** — pasting a job URL into an incident channel shows the recipient
their namespace, not yours. This is the top complaint about the K8s dashboard
and it manifests exactly when it hurts most, mid-incident.

**Option 2 — Namespace in the URL path** (`/jobs/prod/web`).
*Used by:* GitHub (`/org/repo`), Vercel, most multi-tenant products.
*Pros:* links are correct and shareable; browser history and back-button work;
the address bar always answers "where am I"; deep links from CLI output and
alerts land in the right place.
*Cons:* route shapes change (`/jobs/:name` → `/jobs/:namespace/:name`), which
means existing bookmarks need a redirect; no ambient "current namespace" for
pages that are not about a single object.

**Option 3 — Query-parameter filter** (`/jobs?namespace=prod`).
*Pros:* smallest diff; namespace is a facet like any other filter; nothing
about the app becomes modal.
*Cons:* a *filter* is not an *identity* — once two namespaces can both contain
`web`, `/jobs/web` is ambiguous and no query parameter fixes the detail route.
Works for list pages, fails for detail pages.

**Option 4 — Column and facet only, no switcher; everything cross-namespace by
default.**
*Pros:* the operator view is the default view, which is very Legion — one
cluster, one screen. No modality at all.
*Cons:* the tenant experience is bad: a team of six sees a cluster of six
hundred jobs and filters every time. And it puts other tenants' names in front
of people who have no business seeing them.

**Option 5 — Full workspace switch that reloads the app.**
*Used by:* Grafana orgs, Datadog.
*Pros:* unambiguous; strong "you are somewhere" affordance.
*Cons:* heavyweight; the operator loses the cross-tenant view entirely, which
is the one view an operator most needs.

### Recommendation: 2 + 1, with 4 available as a mode

**Namespace lives in the URL; a header switcher sets it; "All namespaces" is a
first-class selection.**

- **Routes** gain the namespace where identity requires it:
  `/jobs/:namespace/:name`, `/rollouts/:namespace/:id`,
  `/services/:namespace/:name`. Existing `/jobs/:name` URLs keep working via a
  redirect that resolves the name in the active namespace (and, while names
  are still globally unique in Phase 1, resolves it outright). Nobody's
  bookmark breaks.
- **List pages** carry the namespace as a query parameter
  (`/jobs?ns=prod`, `ns=*` for all) — a filter, expressed as a filter.
- **The switcher sits in the page header, not the sidebar.** The sidebar is
  navigation between *kinds of thing*; namespace is a property of what you are
  looking at. Putting it in the header keeps it next to the content it scopes,
  and lets it disappear on pages where it does not apply.
- **On cluster-scoped pages** (Nodes, Manager, Raft, Observability, State
  Explorer) the switcher is **absent**, not greyed out, and the page header
  carries a small "cluster-scoped" tag. A control that appears everywhere but
  works in half the places is worse than one that appears where it works.
  Network is the one page that is *neither*: a net policy is a cluster object
  whose selector names namespaces (`ROUTE_RESOURCE['/network'] = 'net'`,
  `ui/src/utils/acl.ts:189`), so it keeps the switcher as a **filter over which
  policies apply to the selected namespace**, and says so in the header rather
  than pretending the page is scoped.
- **"All namespaces" is a real selection**, not a checkbox: it shows a
  namespace column, groups by namespace where grouping helps (the Jobs table),
  and is the default for anyone whose token can read more than one namespace.
- **Single-namespace users never see tenancy UI.** If the caller's effective
  rules grant read on exactly one namespace, the switcher is not rendered at
  all and the namespace appears as a static badge in the header. A developer
  on a one-team cluster should not have to learn a concept that has one
  possible value. The client-side ACL mirror (`ui/src/utils/acl.ts`) already
  has the rules needed to compute this; it gains `namespace` in `jobScope` and
  a `readableNamespaces()` helper.
- **Every detail page header shows `prod / web`** with the namespace as a link
  to that namespace's filtered view. The on-call responder's job to be done —
  "tell me who owns this" — is answered by looking at the top of the page.
- **A Namespaces page** under a new "Governance" sidebar section lists
  namespaces with job counts, description, quota usage (Phase 5), and links
  to the policies that apply to each — which is also the blast-radius view
  from § 7.4 rendered.

Sketch of the header on a job detail page:

```
┌────────────────────────────────────────────────────────────────────┐
│  Jobs ▸ prod ▸ web                                    [prod ▾]     │
│  ┌──────┐                                                          │
│  │ web  │  prod   running   6/6 healthy   high   updated 3m ago    │
│  └──────┘                                                          │
└────────────────────────────────────────────────────────────────────┘
```

The switcher is disabled (not hidden) on a detail page — you cannot "switch"
the namespace of the thing you are looking at; clicking it navigates to that
namespace's job list. That is the one piece of the interaction the Nomad UI
gets wrong, where changing the dropdown on a detail page silently strands you.

---

## 10. Proto, struct, config and persistence changes

### Proto (`proto/legion.proto`)

New messages and RPCs, each with the full doc comments the repo requires:

```proto
message Namespace {
  string name = 1;
  string description = 2;
  map<string, string> meta = 3;
  int64 created_at = 4;
  int64 updated_at = 5;
  // Phase 5:
  // NamespaceQuota quota = 6;
  // repeated string allowed_pools = 7;
  // repeated string allowed_runtimes = 8;
}

rpc ListNamespaces(ListNamespacesRequest) returns (ListNamespacesReply);
rpc GetNamespace(GetNamespaceRequest) returns (GetNamespaceReply);
rpc UpsertNamespace(UpsertNamespaceRequest) returns (UpsertNamespaceReply);
rpc DeleteNamespace(DeleteNamespaceRequest) returns (DeleteNamespaceReply);
```

Field additions, all `optional string namespace` (absent = `default`, so old
clients keep working):

- Job addressing: `RunJobRequest`, `StopJobRequest`, `DestroyJobRequest`,
  `GetJob*`, `RestartJob*`, `ScaleJob*`, `PlanJob*`, `ValidateJob*`,
  `RunJobTemplateRequest`, `RenderJobTemplateRequest`.
- Filtering: `ListJobsRequest` (accepts `*`), `ListInstancesRequest`,
  `ListRolloutsRequest`, `ListServicesRequest`, `ListEventsRequest`,
  `StreamEventsRequest`.
- Replies: `Job`, `JobConfig`, `Instance`, `Allocation`, `Rollout`, `Service`,
  `Event` gain a non-optional `string namespace`.
- `WhoAmIReply` gains `repeated string readable_namespaces` so the CLI and UI
  can render the switcher and pick a sensible default without probing.

The proto comment on `Namespace` states the isolation non-goal in the first
paragraph, because the proto is the API's source of truth and that is the
misunderstanding this feature will generate.

### Structs

- `src/structs/namespace.rs` — `Namespace`, validation, `DEFAULT_NAMESPACE`
  (moved here from `acl.rs`, re-exported for compatibility).
- `src/structs/job.rs` — `namespace: Option<String>` on the jobspec, plus job
  name validation on the write path.
- `src/structs/acl.rs` — `AclResource::Namespace`; `AclScope::job`,
  `::exec`, `::action`, `::job_via_template` take a namespace;
  `AclScope::namespace(name)`.

### Persistence (`src/storage/migrations.rs`)

```sql
CREATE TABLE IF NOT EXISTS namespace (
  name STRING PRIMARY KEY,
  description STRING NOT NULL DEFAULT '',
  meta JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
INSERT OR IGNORE INTO namespace (name, description, created_at, updated_at)
  VALUES ('default', 'Default namespace', ...);

ALTER TABLE job ADD COLUMN namespace STRING NOT NULL DEFAULT 'default';
CREATE UNIQUE INDEX idx_job_namespace_name ON job(namespace, name);
CREATE INDEX idx_job_namespace ON job(namespace);

-- Phase 1 only; dropped in Phase 2 when per-namespace uniqueness takes over.
CREATE UNIQUE INDEX idx_job_name_global ON job(name);

ALTER TABLE idempotency_record ADD COLUMN namespace STRING NOT NULL DEFAULT 'default';
-- new composite uniqueness on (namespace, key)
```

Everything else keys off `job_id` and is untouched. `store.get_job(name)`
becomes `get_job(namespace, name)`; the old signature is removed rather than
kept as a wrapper, so the compiler finds every caller.

Raft: the `namespace` table replicates on the control lane like every other
control-plane table; no new lane, nothing multi-raft-specific.

### Config

Manager config gains nothing in Phases 0–4. Phase 5 adds nothing either —
quota and pool restrictions live on the namespace object, not in the manager
config, which is the whole point of having the object.

### Agent surfaces

- `catalog()` in `src/cmd/capabilities.rs` gains the four namespace commands
  (`namespace list|get|apply|delete`, the last two `mutates: true`).
- MCP (`src/cmd/mcp.rs`) gains `legion_namespace_list` / `legion_namespace_get`
  (read-only, always listed) and `legion_namespace_apply` /
  `legion_namespace_delete` (`mutates: true`, gated behind `--allow-write`).
  Every existing job tool gains an optional `namespace` argument.
- `docs/agent-readiness.md` gains the namespace section.

---

## 11. Implementation phases

Each phase ships alone and is worth shipping alone.

### Phase 0 — The object (no behavior change)

Namespace table, struct, validation, four RPCs, `legion namespace` CLI, MCP
tools, capabilities catalog, `AclResource::Namespace`, ACL endpoint-table
entries. Every job still lands in `default`; nothing else changes.

*Value on its own:* an admin can create namespaces and write policies against
them, and `legion acl policy apply` can start validating that a `namespaces`
selector names a namespace that exists — which kills the silent-no-op footgun
(§ 1) before any of the rest lands.

### Phase 1 — Jobs carry a namespace; scoping is real

Jobspec field, `-n`/`LEGION_NAMESPACE`/`--all-namespaces`, qualified
`namespace/name` references, job-name validation, namespace stamped on every
job scope and every job-derived scope (exec, action, scale, boot, instance,
rollout), list filtering, `$LEGION_NAMESPACE` in task environments,
namespaced idempotency keys, `{namespace}` available in identity subject
templates, policy-apply and namespace-create warnings.

**Names stay globally unique in this phase**, but *resolution is already
namespace-scoped* — `legion job status web` looks in the active namespace and
returns not-found if `web` lives elsewhere. This is the plan's one behavioral
change and it lands here, once.

*Value on its own:* real multi-tenancy for permissions, listing and
governance. Everything except two teams sharing a name.

### Phase 2 — Per-namespace name uniqueness

Drop the global unique index; identity is `(namespace, name)`. Flip the
default workload-identity subject template to include `{namespace}` (§ 7.5) —
same phase, not later, because it is a security dependency of this change.

*Value on its own:* team A and team B both run `web`. Because resolution was
already namespace-scoped in Phase 1, **nothing that worked before stops
working**: this phase only permits things that were previously rejected.

### Phase 3 — Discovery and events

Namespaced service catalog, same-namespace `connect` resolution,
cross-namespace upstreams with explicit intentions, namespace on events plus
per-item event filtering for tenants with `job:read`.

*Value on its own:* namespaces isolate discovery and observability, not just
authorization — the two places a tenant would otherwise still see and reach
another tenant's work.

### Phase 4 — The remaining namespaced objects, and the UI

Volumes and templates become namespaced (with a `shared` namespace for
platform-published templates). UI: routes, header switcher, All-namespaces
mode, Namespaces page, badges, single-namespace suppression (§ 9).

*Value on its own:* the shared cluster becomes legible.

### Phase 5 — Governance

Quota (cpu/memory/jobs), `allowedPools`, `allowedRuntimes`, per-namespace
default priority band, quota usage in the UI and in `legion namespace get`.
Optional `legion namespace create --with-policies` scaffolding an admin and a
developer policy for the new tenant.

*Value on its own:* one team's bad afternoon stops being everyone's.

---

## 12. Testing plan

**Phase 0** — unit: name grammar, reserved names, `default` undeletable,
non-empty deletion refused, cascade ordering. `tests/integration/acl_endpoints.rs`: four
new endpoints in `endpoint_table` with builders; a scenario where a token
scoped to `namespaces: ['team-*']` may create `team-b` and not `prod`.

**Phase 1** — unit: the ACL matrix with real namespaces (allow in A denies in
B; omitted selector matches both; deny in A does not deny the list gate in B);
reference parsing (`web`, `prod/web`, flag+spec conflict, legacy names with
slashes); job-name validation grandfathering. Integration: a `team-a` token
running the full job lifecycle and being denied every step of it against
`team-b`; `list_jobs` filtered with and without `-A`; idempotency keys
isolated across namespaces (the same key in two namespaces must not
cross-replay). Regression: every existing test must pass unchanged — they all
run in `default`, and any that break mean the migration is not transparent.

**Phase 2** — integration: two namespaces, identical job names, full
lifecycle on both concurrently, no interference in allocations, rollouts,
instances, exec, or the ledger. Security: identical `(job, group, task)` in
two namespaces mint **different** identity subjects.

**Phase 3** — integration: same-named services in two namespaces resolve to
their own endpoints; a `connect` upstream cannot reach the other namespace
without an intention; event streams filtered per namespace; `operator:read`
still sees everything.

**Phase 4** — UI: `ui/src/utils/acl.ts` namespace-scope mirror tests; route
redirect from the legacy `/jobs/:name`; switcher suppressed for
single-namespace identities.

**Phase 5** — unit: quota arithmetic and rejection messages. Integration:
submit rejected at the cap with a message naming the quota; `allowedPools`
rejected at submit, not at placement, so the failure is at the point the user
can fix it.

**Benchmarks.** Phases 0–4 do not touch the scheduler or the plan path;
placement inputs are unchanged. Phase 5's quota admission runs on the submit
path, so it ships before/after numbers per AGENTS.md if it lands anywhere near
the plan loop, and the design should prefer a counter maintained on write over
an aggregate query at admission for exactly that reason.

---

## 13. Rollout and backward compatibility

- **Default-on, no feature flag.** The feature is inert until someone creates
  a second namespace: everything is in `default`, the active namespace is
  `default`, and every existing policy, script, bookmark and dashboard keeps
  working. A flag would only add a state where half the cluster's behavior
  depends on a config value nobody remembers setting.
- **The one behavioral change** is Phase 1's namespace-scoped resolution, and
  it is invisible until a second namespace exists. Phase 2 relaxes rather than
  restricts, so it changes nothing that previously worked.
- **Old clients** omit the namespace field, which means `default` — a mixed
  fleet during upgrade works, with older CLIs simply unable to reach other
  namespaces. Documented in `docs/upgrades.md`.
- **Old workers** ignore `$LEGION_NAMESPACE`; nothing worker-side depends on
  it.
- **The two documented breaking changes**, each with an upgrade note:
  1. Phase 2's workload-identity subject template (IdP trust policies must be
     rewritten; pinnable in config with a startup warning).
  2. Phase 1's job-name validation on the write path (a *new* job cannot
     contain `/`; existing ones are grandfathered).
- **Docs land with the phases, not after.** A new public page,
  `docs/namespaces.md`, covering the surface, the defaults, the failure modes
  (not-found in the wrong namespace, non-empty delete, the omitted-selector
  hazard) and — first and loudest — the isolation non-goal. Updates to
  `docs/auth-and-acls.md` (the `namespaces` selector becomes real; the
  existing `namespaces = ["prod"]` example stops being a no-op),
  `docs/network-handlers.md` (its `selector.namespaces` stops matching every
  job, and the two "until namespaces land" notes come out),
  `docs/service-discovery.md` (the DNS shape), `docs/workload-identity.md`,
  `docs/agent-readiness.md`, `docs/upgrades.md`, `docs/README.md`,
  `README.md`, and `configs/jobs/maximal.ncl`.
- **TODO.md** gets the open item now and the check-offs per phase; the
  "per-namespace band caps" claim gets corrected in the same change as Phase 1.

---

## 14. Open questions

1. **Where does quota get enforced?** At submit against a maintained
   per-namespace counter (fast, can drift), or in the plan path against live
   allocation state (accurate, on the hot path)? *Lean:* both — a cheap
   counter check at submit for the good error message, and a re-check when the
   plan is applied, mirroring how preemption validates at apply time. Resolve
   in Phase 5 with measurements.
2. **Should KV become namespaced after all?** Path prefixes work, but a tenant
   has to be told its prefix, and there is no relationship between namespace
   `team-a` and path `team-a/`. *Lean:* keep them separate, and add an
   optional `kvPrefix` on the namespace object that ACL policy scaffolding
   (`--with-policies`) uses to generate the right `paths` selector. That gives
   the ergonomic win without a second scoping mechanism over the same data.
3. **Do templates belong to a namespace or to the cluster?** Platform teams
   publish templates *for* tenants, so a purely namespaced template store
   forces duplication. *Lean:* namespaced, with a `shared` namespace that any
   identity with `template:read` there may submit through — one extra concept,
   and the alternative (a `shared: true` flag on each template) is the same
   concept with worse ergonomics.
4. **Should `--all-namespaces` be the default for management tokens?** An
   operator almost always wants the cluster view; a tenant almost never does.
   *Lean:* yes for the UI (default to All when the identity can read more than
   one namespace), no for the CLI (scripts must not change behavior based on
   token type). The asymmetry is defensible but deserves a second opinion.
5. **Does a namespace need an owner field with meaning**, or is `meta` enough?
   *Lean:* `meta` only. An `owner` that nothing enforces is documentation with
   a schema.
6. **Cross-namespace job dependencies** (a job in `team-a` waiting on a
   service in `shared`) — the `connect` intention model in § 7.5 covers the
   runtime case, but not a future job-dependency feature. Out of scope here;
   flagged so the dependency work does not assume a flat world.

---

## 15. Acceptance criteria

**Phase 0.** `legion namespace create|list|get|delete` work over CLI, gRPC and
MCP with `--json` envelopes; `default` exists and cannot be deleted; a
non-empty namespace refuses deletion with a message naming what is in it;
`every_manager_rpc_is_classified` passes with the four new RPCs classified;
`legion acl policy apply` rejects (or warns on) a `namespaces` selector naming
a namespace that does not exist.

**Phase 1.** A token scoped to `namespaces: ['team-a']` can run the complete
job lifecycle in `team-a` and receives `PermissionDenied` — not `NotFound` —
on every equivalent operation in `team-b`; `legion job list` shows the active
namespace only and reports how many jobs are hidden elsewhere; `-A` shows all
with a namespace column; the same idempotency key in two namespaces produces
two independent records; every pre-existing test passes unchanged.

**Phase 2.** Two namespaces each run a job named `web`, concurrently, through
submit, rollout, scale, exec and destroy, with no interference; their tasks'
identity tokens carry different subjects; no operation that worked in Phase 1
has changed behavior.

**Phase 3.** Two namespaces each advertise `web`; each namespace's `connect`
upstream reaches its own; a cross-namespace upstream fails without an
intention and succeeds with one; a tenant token streams only its namespace's
events while `operator:read` still sees all of them.

**Phase 4.** A namespaced job's URL, pasted into another operator's browser,
opens the same job; a single-namespace identity sees no switcher anywhere; the
Namespaces page lists every namespace with job counts and the policies that
apply to it.

**Phase 5.** A namespace at its CPU quota rejects the next submit with a
message naming the quota and the current usage; a job constrained to a
disallowed pool is rejected at submit rather than failing to place; quota
usage is visible in `legion namespace get -j` and on the Namespaces page.
