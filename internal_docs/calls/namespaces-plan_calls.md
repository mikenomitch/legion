# Judgement Calls: Namespaces and Namespace-Scoped ACLs

Plan: [`plans/namespaces-plan.md`](../plans/namespaces-plan.md).

**Entries are ranked by project impact, highest first** — how much of the
system the decision constrains, how expensive it would be to reverse, and how
visible it is to users. The entry number is its rank.

These are the calls made while *writing* the plan; the plan has not been
implemented. Anything resolved during implementation, and every deviation from
the plan, gets appended here at its rank.

---

## 1. Job identity becomes `(namespace, name)`, but namespace-scoped *resolution* ships one phase before per-namespace *uniqueness*

**Call.** The end state is a compound identity: two namespaces may each hold a
job called `web`. The path there is split so that Phase 1 stamps the namespace
on every job, scope and list, and makes lookups namespace-scoped — while names
are still globally unique — and Phase 2 then drops the global unique index.

```console
# Phase 1: `web` lives in prod. From the default namespace:
$ legion job status web
error: job 'web' not found in namespace 'default'
hint: a job named 'web' exists in namespace 'prod'; retry with -n prod

# Phase 2 changes nothing about that. It only stops rejecting this:
$ legion job run web.ncl -n dev     # while prod/web already exists
```

**Alternatives.** (a) Ship the compound key in one phase — the honest
modelling answer, and what Nomad and Kubernetes both have. (b) Stop at the
attribute (option C in the plan's comparison): namespace as a label, names
globally unique forever. Cheap, and it delivers policy scoping, governance and
UI grouping — everything except the collision-free names people actually ask
for.

**Trade-off.** Two migrations instead of one, and a window (Phase 1) where the
product has namespaces that do not yet let two teams share a name — the single
most expected property of the feature. Accepted because of what the ordering
buys: the plan's one behavioral change (a lookup that used to find a job now
returns not-found) lands exactly once, in Phase 1, when all jobs are still in
`default` and therefore nothing can observe it. Phase 2 is then a pure
relaxation: it only permits submissions that were previously rejected, so no
script, bookmark, dashboard or policy can break on it.

**Why (simplicity).** A migration nobody can observe is worth more than a
migration that is theoretically atomic. Doing it the other way round — relax
uniqueness first, scope resolution later — would mean unqualified names quietly
changing which job they refer to, which is the failure mode that makes people
distrust a tenancy feature permanently.

---

## 2. Namespaces are explicitly not an isolation boundary, and the docs lead with it

**Call.** A namespace is a naming, authorization and governance boundary.
Compute isolation is node pools (shipped); network isolation is
`network.mode = "gated"` plus net policies (shipped with the mesh — see
[docs/network-handlers.md](../../docs/network-handlers.md)); stronger isolation
is federation (planned). The non-goal is stated first in the public page, in
the proto comment on `Namespace`, and in `legion namespace --help` — not in a
limitations section at the bottom.

**Alternatives.** (a) Make namespaces do isolation work: implicit node-pool
binding, or a namespace that implies `gated` network mode for everything in it.
(b) Say nothing and let users infer, which is what every surveyed system did
first.

**Trade-off.** Users arriving from Kubernetes will ask why namespace `prod`
does not keep its work off `dev`'s machines, and the answer is a second
concept (`allowedPools`, Phase 5) rather than a property of the first. We pay
one extra explanation per new operator. (a) is also the more tempting option
now that `gated` exists and a namespace *could* carry a default network mode —
and it stays rejected: a namespace that silently changes a workload's
networking would make the tenancy boundary the thing that breaks a deploy.

**Why (simplicity).** "A namespace is not a security boundary" is the single
most-repeated correction in the Kubernetes ecosystem, Consul needed admin
partitions on top of namespaces for the same reason, and Nomad had to add node
pools in 1.6 because namespaces did not isolate compute. Three systems learned
it the expensive way. Legion already has the isolation primitive, so it can
say what a namespace is *for* without leaving anyone stranded — which is only
true if we say it loudly enough to be believed.

---

## 3. An omitted `namespaces` selector keeps meaning "all namespaces", and the hazard is made visible instead of special-cased

**Call.** Every existing policy omits `namespaces`, and an omitted selector
means *any* — so on the day a second namespace is created, every existing
policy applies to it. That semantic is kept. The hazard is surfaced at the two
moments it can bite:

```console
$ legion acl policy apply team-a --rules @team-a.ncl
Applied policy 'team-a'.
warning: rule 0 (job:read,write) has no `namespaces` selector — it applies to
         all 4 namespaces, including any created later.

$ legion namespace create team-b --dry-run
Would create namespace 'team-b'.
3 existing policies would apply to it because they name no namespace:
  platform-admin (job:*, kv:*)   ops-readonly (job:read)   ci (job:write)
```

**Alternatives.** (a) Make an omitted `namespaces` selector mean `default`
only — safe by default, and it would mean no existing policy ever silently
gains reach. (b) Reject job rules without a `namespaces` selector once more
than one namespace exists, forcing every policy to be explicit.

**Trade-off.** (a) is genuinely safer and we are not taking it: a policy
written today does gain reach over a namespace created tomorrow, and the only
defense is a warning someone has to read. Accepted, with the blast-radius
report at `namespace create` as the compensating control — the moment the
reach is actually granted is the moment the operator is shown who has it.

**Why (simplicity).** The entire ACL engine rests on one rule: *a rule with a
selector matches only requests carrying that attribute; a rule without one
matches everything*. It is stated in `src/structs/acl.rs`, in
`docs/auth-and-acls.md`, in the proto, and mirrored in the UI's client-side
evaluator. Carving out one selector to mean something else makes that sentence
false, and every future reader has to remember which selector is the exception.
An exception in a security model is worse than a documented hazard with a
warning at both edges.

---

## 4. The workload-identity subject template gains `{namespace}` in the same phase that relaxes name uniqueness

**Call.** `{namespace}` becomes an available placeholder in Phase 1, and the
cluster default subject template changes from
`{region}:{job}:{group}:{task}` to `{region}:{namespace}:{job}:{group}:{task}`
in Phase 2 — the phase that first allows two namespaces to hold the same job
name. Not later, not behind a flag that defaults off.

**Alternatives.** (a) Leave the default template alone and let operators opt
in, avoiding a breaking change to every IdP trust policy in existence.
(b) Change it in Phase 1, before it is strictly required, getting the break
over with earlier.

**Trade-off.** A documented breaking change: every trust policy at every IdP
that matches on `sub` has to be rewritten at the Phase 2 upgrade. Clusters
that cannot take it can pin the old template in config, and get a startup
warning naming the risk once a second namespace exists.

**Why (security beats compatibility here).** With per-namespace names and a
namespace-free subject, `prod/web` and `dev/web` mint **identical** JWT
subjects. An IdP policy written to trust the production workload would accept
a token minted for the development one. That is a forgery path created by a
feature whose entire purpose is separating tenants, so it cannot ship as an
opt-in. (a) would have left it opt-in, which means shipping the vulnerability
as the default.

---

## 5. Namespace CRUD is a new ACL resource, not part of `operator`

**Call.** Add `AclResource::Namespace`, scoped by the existing `namespaces`
selector, so "a team lead may create `team-a-*` namespaces" is expressible:

```nickel
{ resources = ["namespace"], actions = ["write"], namespaces = ["team-a-*"] }
```

**Alternatives.** (a) Gate namespace CRUD on `operator:write`, adding no
vocabulary. (b) Gate it on `acl:write`, since a namespace is arguably part of
the authorization model.

**Trade-off.** A thirteenth resource in a list the plan elsewhere argues should
stay short, and one more row in the docs table and the endpoint test.

**Why (simplicity, via precedent).** `operator` means raft, manager configs
and observability; folding namespaces into it makes everyone who may create a
tenant a raft administrator, which is the exact argument the preemption work
used to reject `operator:write` as the gate for priority bands. `scale` and
`boot` were both split out for the same reason. Following the established
precedent costs one enum variant; breaking it costs a permanent explanation.

---

## 6. Deletion refuses while non-empty, with a synchronous `--cascade`; there is no `Terminating` state

**Call.**

```console
$ legion namespace delete staging-old
error: namespace 'staging-old' is not empty: 3 jobs, 1 volume
hint: `legion job list -n staging-old` to see them, or re-run with --cascade
```

`--cascade` stops and destroys contents synchronously, reporting each step. A
cascade that fails halfway leaves the namespace present, deletable, and
inspectable.

**Alternatives.** (a) Kubernetes' model: deletion always cascades, driven by
finalizers and an asynchronous `Terminating` phase. (b) Refuse always, with no
cascade at all — force the operator to empty it by hand.

**Trade-off.** No asynchronous cleanup means `--cascade` on a large namespace
blocks for as long as the work takes, and a client timeout leaves the operator
re-running it. Accepted: a re-runnable command that reports progress is easier
to reason about mid-incident than a background reconciler.

**Why (simplicity).** "Namespace stuck in Terminating" is one of the most
searched Kubernetes failure modes, and its documented fix is hand-editing an
object's finalizer list. The asynchronous model buys correctness under
concurrent writes and pays with a state that operators cannot get out of. A
refusal is a state you can always get out of.

---

## 7. A flag/jobspec namespace conflict is an error, not a precedence rule

**Call.**

```console
$ legion job run api.ncl -n dev      # api.ncl declares namespace = "prod"
error: jobspec declares namespace 'prod' but -n specified 'dev'
hint: remove one; a jobspec that names its namespace does not need the flag
```

**Alternatives.** (a) Nomad's rule: the flag wins. (b) The jobspec wins.
Either is one sentence of documentation and no error path.

**Trade-off.** One more rejection a user can hit, in a place where a
precedence rule would have "just worked".

**Why (simplicity / blast radius).** The two inputs were written by the same
person in the same breath, and they disagree. Silently picking one is how a
staging deploy lands in production, and neither choice is more obviously
correct than the other — which is the tell that there is no right precedence
rule to learn. kubectl errors here; Nomad does not; we follow kubectl.

---

## 8. The UI puts the namespace in the URL and the switcher in the page header

**Call.** Detail routes carry the namespace (`/jobs/:namespace/:name`), list
pages carry it as a query parameter (`/jobs?ns=prod`, `ns=*`), and the
switcher lives in the page header rather than the sidebar. Legacy `/jobs/:name`
URLs redirect by resolving in the active namespace. On cluster-scoped pages
(Nodes, Raft, Manager, Observability, State) the switcher is **absent**, not
disabled.

**Alternatives.** (a) Sidebar switcher with the selection in localStorage —
what the Kubernetes Dashboard and the Nomad UI do. (b) Query-parameter filter
only, no route change: the smallest possible diff.

**Trade-off.** Route shapes change, so bookmarks need a redirect layer, and
list pages and detail pages express the namespace two different ways (query
param vs path segment) — a slight inconsistency in exchange for each one
matching what it actually is: a filter on a list, an identity component on an
object.

**Why (the incident matters more than the diff).** A namespace selection that
lives only in browser storage produces links that show the recipient *their*
namespace, not yours — which fails precisely when someone pastes a job URL into
an incident channel. (b) cannot work at all once two namespaces hold the same
job name: `/jobs/web` becomes ambiguous and no query parameter fixes a detail
route.

---

## 9. Single-namespace identities never see tenancy UI at all

**Call.** If the caller's effective ACL rules grant read on exactly one
namespace, the UI renders no switcher — the namespace appears as a static badge
on detail headers and nothing else. `WhoAmIReply` grows
`readable_namespaces` so the client can decide without probing.

**Alternatives.** (a) Always show the switcher, disabled or with one entry —
consistent, and the control is where users expect it once they grow a second
namespace. (b) Hide it only when the cluster has one namespace, rather than
when the *caller* can see one.

**Trade-off.** The UI differs between identities on the same cluster, so two
people screen-sharing may see different chrome, and a user who gains a second
namespace sees a control appear that was never there before.

**Why (simplicity).** Legion's headline claim is being the simplest scheduler
to run, and the overwhelmingly common case is one team on one cluster. A
developer on a single-tenant cluster should never have to learn a concept that
has exactly one possible value. (b) is the wrong axis: what matters is what
*this caller* can act on, not what exists.

---

## 10. Governance lives on the namespace object, while priority bands stay in ACL rules

**Call.** Quota, `allowedPools` and `allowedRuntimes` are fields on the
namespace (Phase 5). Priority bands stay where the preemption work put them —
the `bands` selector on `job` ACL rules. Two surfaces, deliberately.

**Alternatives.** (a) Put everything on the namespace, including band caps —
one governance surface, and it retires an ACL selector. (b) Put everything in
ACLs, including quota, by inventing a quota-bearing rule.

**Trade-off.** An operator has to know which knob lives where, and "why is the
band cap not on the namespace like the CPU cap?" is a fair question that needs
an answer every time it is asked.

**Why (they answer different questions).** ACL rules answer *may this
principal do X*; namespace governance answers *may anything in this tenant do
X*. A quota a principal could dodge by switching tokens is not a quota, so it
cannot live on a rule. Conversely, the interesting band question genuinely is
per-principal — "the autoscaler may claim `high`, humans may not" — and
[preemption call 5](preemption-and-eviction-plan_calls.md) already rejected a
`priority.maxBandByNamespace` config map on the grounds that it would ship a
second policy vocabulary. Moving bands onto the namespace now would re-make
exactly that mistake in the other direction.

---

## 11. KV stays path-scoped; namespaces do not own a KV subtree

**Call.** KV keeps its flat path space and its `paths` ACL selector. The
namespace object may later carry an optional `kvPrefix` used only by policy
scaffolding to generate the right `paths` selector — the namespace never
becomes an implicit prefix on KV reads and writes.

**Alternatives.** (a) Namespace the KV tree the way Kubernetes namespaces
Secrets and ConfigMaps, so `team-a`'s keys are unreachable from `team-b` by
construction. (b) Auto-bind every namespace to a `<name>/` prefix and rewrite
keys transparently.

**Trade-off.** A tenant has to be *told* its KV prefix; nothing in the system
enforces the relationship between namespace `team-a` and path `team-a/`. Shared
configuration across tenants stays easy, which is either a feature or a hole
depending on who is asking.

**Why (simplicity).** KV already has a working, documented, tested scoping
mechanism that predates this plan. Adding a second one over the same data means
every KV question acquires a "which mechanism applies here" preamble, and (b)
in particular — transparent key rewriting — makes the key a client sees differ
from the key the store holds, which is the kind of indirection that turns a KV
debugging session into an archaeology dig. Revisit only on a concrete request
(plan § 14, open question 2).

---

## 12. Qualified references use `namespace/name`, and new job names may not contain a slash

**Call.** `prod/web` is accepted anywhere a job name is accepted and is the
display form in `-A` listings, the UI and structured output. Job names get
validation on the write path from Phase 1 — the namespace grammar plus dots and
underscores — with rows that already exist grandfathered.

**Alternatives.** (a) `web@prod`, Nomad-flavored and unambiguous without any
new name validation, since `@` is even less likely in a job name.
(b) `-n` only, with no qualified form at all — nothing to parse, and no
ambiguity to create.

**Trade-off.** Job names are barely validated today (a length check and
nothing else in `src/structs/job.rs`), so this makes a previously-legal name
illegal for *new* jobs, and a grandfathered job with a slash is addressable
only with `-n`. That is a real, if tiny, compatibility break in exchange for an
unambiguous parse.

**Why (ease, and matching what people already type).** `ns/name` is the form
Kubernetes output uses, it sorts by tenant, and it matches the `/`-separated
path idiom Legion already uses for KV. (b) is the safe answer but it means
neither an agent nor an alert can emit a single copy-pasteable identifier, and
"print something a human can paste back" is a stated agent-readiness goal.
