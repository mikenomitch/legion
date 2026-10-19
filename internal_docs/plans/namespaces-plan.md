# Plan: Namespaces

> **Judgement calls:** [`calls/namespaces-plan_calls.md`](../calls/namespaces-plan_calls.md)

**Status: SHIPPED** (2026-10-19), all four phases, with the deviations
below. The public page is [`docs/namespaces.md`](../../docs/namespaces.md);
this plan stays as the record of why. Before it, the ACL rule vocabulary had
a `namespaces` selector and the scope struct a `namespace` field, both fed
the constant `"default"`, so a namespace was vocabulary with no values. The
plan also says, with sources, which parts of Kubernetes' and Nomad's
namespaces Legion is **not** copying.

**Where the build differs from this text** (each is a call in the calls doc,
which wins over the sections below):

- **The canonical id is bare in `default`** (`web`, `payments-prod/web`), and
  there are no separate `namespace`/`name` columns: the namespace is split
  off the id (call 1). Consequently there is **no refusal migration**
  (call 18): existing ids are already correct.
- **Server job fields take the canonical id**; only spec-bearing requests
  carry a `namespace` field, and lists take `namespaces` (call 5). Replies
  other than `Job`, services and events carry no separate `namespace` /
  `job_name` (call 30).
- **A stored rule without `namespaces` reaches `default` only**, and `*`
  resource and deny rules are exempt from the selector requirement (call 4).
- **Quota charges max(declared, live)** inside the control-lane submit, with
  no plan-apply re-check (call 7). Scale-out and instance create are
  pre-checks; unnamed sessions are not charged (call 29).
- **Content-store paths spell the namespace as a segment**, `default`
  included, and the fence prefix is `sessions/_fence/` (call 11).
- **Metrics** are new `legion_namespace_*` families plus a `namespace` label
  on preemptions, not a label on every family (call 28).
- **Standalone volumes** need no work: every volume row is created by a job
  and carries its canonical id; a jobless (legacy) row is visible only under
  `namespaces = ["*"]`.

Still deferred, beyond the list below: per-namespace keyed instance ids for
*minted* ids (they stay cluster-unique by randomness, call 10), and the
`ValidateJob` / volume-create namespace fields (`job validate` checks a spec
locally; volumes are created only by jobs).

**Revised 2026-10-18.** The first version (2026-09-29) predates two things:

- AGENTS.md's rule that Legion has no installed base to protect (2026-10-11).
- A sourced survey of how namespaces actually fail in Kubernetes and Nomad.

This revision:

- Drops the two-step identity migration.
- Makes the namespace part of every job-keyed storage key from the first phase.
- Namespaces services in the same phase as jobs.
- Requires ACL rules to name their namespaces.
- Moves quota off the namespace object and onto glob-selecting namespace
  policies, so delegated namespace creation cannot dodge a quota.

It also corrects claims that the code had outrun: idempotency keys are already
scoped per caller, and job names already forbid `/`. Each reversal keeps its
superseded reasoning in the calls doc.

Deliberately deferred, each restated in § 5 or § 12:

- Hierarchical namespaces.
- Namespace rename, and moving a job between namespaces.
- Namespaced KV.
- Per-namespace scheduler isolation.
- Tenant-authored net policies.
- A persisted CLI namespace context.
- Namespace replication across federated regions.

---

## 0. The plan on one page

**A namespace is a name scope, a permission scope, and a policy anchor. It
is not a wall.** Two namespaces can each hold a job called `web`. A token can
be confined to one namespace. A quota can cap a group of namespaces. Nothing
about creating a namespace moves a workload to different machines, a
different network, or a different kernel. That stays the job of node pools,
`gated` networking and the `sandbox` runtime, which Legion already has.

```console
$ legion namespace create payments-prod --description "Payments, production"
Created namespace payments-prod.

$ export LEGION_NAMESPACE=payments-prod
$ legion job run web.ncl
Submitted payments-prod/web (version 1).

$ legion job status web              # resolved in the active namespace
$ legion job status checkout-dev/web # qualified: works from anywhere
$ legion job list -A                 # every namespace you can read
NAMESPACE       NAME   STATUS   ...
checkout-dev    web    running
payments-prod   web    running
```

```nickel
# The payments team's whole policy. A resource Legion adds next year is
# covered by adding it to `resources`, not by inventing another selector.
[
  { resources = ["job", "exec", "action", "scale", "boot", "snapshot"],
    actions = ["read", "write"], namespaces = ["payments-*"] },
  { resources = ["namespace"], actions = ["read", "write"],
    namespaces = ["payments-*"] },     # the team creates its own environments
]
# No `node:read`: the ledger behind it shows every tenant's job ids (§ 8.6).
```

| Namespaced (identity is `namespace/name`) | Cluster-scoped |
| --- | --- |
| jobs, job versions, instances, allocations, rollouts, scale, volumes, snapshots, boot/exec/session tickets, **services and their DNS names**, workload-identity subjects, events | nodes, node pools, ACL policies/tokens, job templates, net policies, KV, signing keys and CAs, saved queries, the state explorer |

Phases, each shippable alone (§ 12):

1. **Namespaces are real.** The object, delegable CRUD, `(namespace, name)`
   job identity everywhere, scoped tokens, same-namespace service discovery,
   distinct workload identities, and the minimum UI routing.
2. **Crossing on purpose, and seeing your own.** Explicit cross-namespace
   upstreams and DNS names, tenant-readable events, namespace metric labels,
   and namespaced exec keys and standalone volumes.
3. **The UI.** The switcher, the All-namespaces view, and the Namespaces page.
4. **Governance.** Namespace policies: an aggregate quota per priority band,
   allowed pools, runtimes and host volumes.

---

## 1. Problem statement

Legion has no tenancy boundary. Every job, service, instance, volume,
template, KV key and saved query lives in one flat global name space. The
only tool for dividing it is a naming convention enforced by ACL globs.

Here is what a platform team has to do today to give two teams one cluster:

```bash
# Tell both teams, in a wiki page, to prefix every job name with their team.
legion acl policy apply team-a --rules '[
  {"resources": ["job"],      "actions": ["read","write"], "names": ["team-a-*"]},
  {"resources": ["exec"],     "actions": ["write"],        "names": ["team-a-*"]},
  {"resources": ["action"],   "actions": ["write"],        "names": ["team-a-*"]},
  {"resources": ["scale"],    "actions": ["read","write"], "names": ["team-a-*"]},
  {"resources": ["snapshot"], "actions": ["read","write"], "names": ["team-a-*"]},
  {"resources": ["kv"],       "actions": ["read","write"], "paths": ["team-a/*"]}
]'
# ...and again for team-b, and again every time Legion grows a resource type.
```

What breaks:

- **The prefix is the tenancy boundary, and the system can't see it.**
  `legion job list` shows both teams' jobs interleaved. Legion never
  validates, displays or groups by the prefix.
- **Every new resource needs a new line in every tenant policy.** `scale`,
  `boot`, `exec`, `action` and `snapshot` each arrived as a new resource, and
  `templates`, `actionNames` and `bands` as new selectors. Nothing in a
  policy names the *tenant*, so nothing extends to cover a resource that did
  not exist when the policy was written.
- **Two teams can't both run a job called `web`.** They rename to
  `team-a-web` and `team-b-web`, and every dashboard, alert and runbook
  carries the prefix forever.
- **There is nowhere to hang tenant-wide governance.** Quotas, allowed node
  pools and "this tenant may not use the `exec` runtime" each have no object
  to attach to. The preemption work already rejected one such config map,
  `priority.maxBandByNamespace`, on the grounds that the namespace was the
  right anchor
  ([preemption plan open question 5](preemption-and-eviction-plan.md)). That
  anchor does not exist yet.
- **Several global name spaces leak between tenants once tenants exist:**
  - *The service catalog pools endpoints by name across jobs.* A service's
    name is `COALESCE($.name, task name)` (`src/structs/network.rs:583`).
    `service_endpoints` returns every job's endpoints for it
    (`src/storage/store.rs:15066`). So a second team that also advertises
    `web` adds its instances to the first team's load-balancing pool.
  - *Every service read is visible to every reader.* `ListServices`,
    `ResolveService` and `ListServiceEdges` gate on `job:read` with
    `AclScope::any()` and filter nothing (`src/manager/server.rs:10197-10415`).
    Any `job:read` grant sees every endpoint address in the cluster.
  - *Instance ids are caller-chosen and unique cluster-wide*
    (`src/storage/migrations.rs:458`, `src/storage/store.rs:16977`). The
    second team to pick `user-42` gets `AlreadyExists`, which tells it that
    some other team has a `user-42`.
  - *Checkpoint seal keys and chunk dedup derive from the bare job id*
    (`src/checkpoint_seal.rs:320-345`, `:481`). That is harmless
    while names are unique, and a shared key the day they stop being unique.

Two more are sharper:

- **A namespace-scoped policy silently grants nothing.** An admin who sees
  `namespaces` in the selector list writes
  `{"resources": ["job"], "actions": ["*"], "namespaces": ["prod"]}`. The
  policy authorizes nothing, and there is no error or warning. Every scope in
  the product is stamped `"default"`, and a selector that names a value the
  request never carries fails closed. The docs say so
  (`docs/auth-and-acls.md:186-191`), but this footgun gets discovered at 3am.
- **A `net` rule with `namespaces` matches nothing at all.** Net-policy
  upsert, get, list and delete build their scope from the policy's node pools
  only (`src/manager/server.rs:13518-13530`). Footnote 1 of the first version
  of this plan said the opposite.

---

## 2. Why namespaces: the benefits, and where they are oversold

This section answers "is it worth building at all". The evidence is sorted
by how strong it is, and each benefit says where the field has found it to
be weaker than it sounds.

| # | Benefit | Evidence | Where it is oversold |
| --- | --- | --- | --- |
| B1 | **Sharing one cluster is much cheaper than running many.** | The Borg paper: separating prod from non-prod "would need 20–30% more machines in the median cell". Splitting large users into their own cells needs "2–16× as many cells, and 20–150% additional machines". Sharing costs "about 3%" in CPU performance ([Borg, EuroSys 2015](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/43438.pdf) §5). Twine (Meta) found that siloed clusters "strand unused capacity" ([OSDI 2020](https://www.usenix.org/system/files/osdi20-tang.pdf)). An EKS control plane costs $0.10–0.60 per hour before any operations work ([pricing](https://aws.amazon.com/eks/pricing/)). | Sharing is what saves money, not namespaces. Namespaces are what make sharing *tolerable*. Organizations still run many clusters: half of CNCF end users run more than 10 ([CNCF 2022](https://www.cncf.io/reports/cncf-annual-survey-2022/)). Hard isolation still means separate clusters ([K8s multi-tenancy docs](https://kubernetes.io/docs/concepts/security/multi-tenancy/)). |
| B2 | **Names don't collide, and environments match.** The same spec lands in staging and prod, differing only in where it lands. | K8s: names are "unique within a namespace, but not across namespaces". Kustomize's `namespace:` field exists to promote one manifest unchanged ([docs](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/namespace/)). The counter-example is Fly.io: app names are global, and users say that "undermines the purpose of organizations" ([forum](https://community.fly.io/t/namespace-fly-app-names/6264)). | K8s itself: "for clusters with a few to tens of users, you should not need to create or think about namespaces at all" ([namespaces docs](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/)). |
| B3 | **Contained blast radius for people and automation.** A CI token or an AI agent confined to one namespace cannot damage another. | AWS: isolation means a "misconfiguration, or malicious activity… within one account" stays contained ([whitepaper](https://docs.aws.amazon.com/whitepapers/latest/organizing-your-aws-environment/benefits-of-using-multiple-aws-accounts.html)). Argo CD project roles confine CI to destination namespaces ([docs](https://argo-cd.readthedocs.io/en/release-2.12/user-guide/projects/)). OWASP LLM06 says to limit an agent's permissions "to the minimum necessary" ([LLM06](https://genai.owasp.org/llmrisk/llm062025-excessive-agency/)). Red Hat's Kubernetes MCP guide offers a namespace-scoped binding "for a tighter scope" ([guide](https://developers.redhat.com/articles/2025/09/25/kubernetes-mcp-server-ai-powered-cluster-management)). | GKE: "Kubernetes cannot guarantee perfectly secure isolation between tenants" ([overview](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/multitenancy-overview)). Anyone who can create Services can man-in-the-middle traffic across namespaces ([CVE-2020-8554](https://discuss.kubernetes.io/t/security-advisory-cve-2020-8554-man-in-the-middle-using-loadbalancer-or-externalips/14003)). |
| B4 | **Delegated self-service.** A team lead creates their own environments without filing a ticket. | Delegation is a stated design goal in the [K8s namespace proposal](https://github.com/kubernetes/design-proposals-archive/blob/main/architecture/namespaces.md), [Consul](https://developer.hashicorp.com/consul/docs/multi-tenant/namespace) and [Vault](https://developer.hashicorp.com/vault/docs/enterprise/namespaces). HNC was built because "whenever a team wants a new namespace, they must raise a ticket" ([K8s blog](https://kubernetes.io/blog/2020/08/14/introducing-hierarchical-namespaces/)). | No one has published before-and-after onboarding numbers. The evidence is design intent. |
| B5 | **One anchor for governance:** quotas, default and allowed pools, allowed runtimes. | K8s ResourceQuota exists because "one team could use more than its fair share" ([docs](https://kubernetes.io/docs/concepts/policy/resource-quotas/)). Nomad namespaces carry driver allowlists and `node_pool_config` "checked at job submission" ([spec](https://developer.hashicorp.com/nomad/docs/other-specifications/namespace)). Cloud Foundry entitles orgs to isolation segments ([docs](https://docs.cloudfoundry.org/adminguide/isolation-segments.html)). | Vault: "most of the desired level of isolation can be enforced via ACL policies", so use namespaces sparingly ([structure guide](https://developer.hashicorp.com/vault/docs/enterprise/namespaces/namespace-structure)). Policies that only exist at namespace level push users to one namespace per object; Pulsar had to add topic-level overrides ([PIP-39](https://apache.googlesource.com/pulsar/+/HEAD/pip/pip-39.md)). |
| B6 | **Cost allocation.** | "Tenants are primarily associated with namespaces (40%) or labels (25%) to allocate costs" ([CNCF/FinOps 2021](https://cncf.io/wp-content/uploads/2021/06/FINOPS_Kubernetes_Report.pdf)). OpenCost aggregates by namespace ([spec](https://opencost.io/docs/specification)). | Idle and shared costs still need distribution rules, and 24% of respondents do not monitor Kubernetes spend at all (same report). |
| B7 | **Observability scoped to a tenant.** | prom-label-proxy enforces a `namespace` label on queries ([repo](https://github.com/prometheus-community/prom-label-proxy)). Temporal namespaces carry their own rate limits ([guide](https://docs.temporal.io/best-practices/managing-namespace)). | The mapping from namespace to tenant needs extra proxies; no metrics store treats them as the same thing natively. |
| B8 | **Legibility, and a word everyone already knows.** | 71% of CNCF respondents separated teams with namespaces; 15% used "only labels" ([CNCF 2018](https://www.cncf.io/blog/2018/08/29/cncf-survey-use-of-cloud-native-technologies-in-production-has-grown-over-200-percent/)). | Recognition is not a technical argument. It is not nothing either. |

**What this means for Legion.** B1–B3 are what Legion's single-cluster,
single-binary pitch needs most. They are Phase 1. B4 and B5 are the
governance half (Phases 1 and 4), and the research reshaped their design
(§ 8.12). B6 and B7 come almost for free once every workload carries a
namespace (Phase 2). The honest summary, which the public page will lead
with: **namespaces buy organization and authorization. Isolation comes from
pools, `gated` and `sandbox`.**

---

## 3. Prior art: how namespaces go wrong

Each complaint below has a source and a strength-of-evidence note. The last
column is Legion's answer, with a pointer into this plan, so every complaint
traces to a design decision or an explicit non-goal.

### 3.1 Kubernetes

**The model.** Namespaces are cluster-scoped objects. Most resources live in
one; Nodes, PersistentVolumes, StorageClasses, CRDs and ClusterRoles do not.
Names are unique per namespace, and DNS encodes it as
`svc.ns.svc.cluster.local`. RBAC is a 2×2 grid: Role or ClusterRole, bound by
a RoleBinding or a ClusterRoleBinding. ResourceQuota and LimitRange attach
per namespace. Deletion cascades through finalizers and a `Terminating`
phase.

| # | Complaint | Evidence | Legion's answer |
| --- | --- | --- | --- |
| K1 | **A namespace is not a security boundary.** It doesn't cover the network (allow-all by default; a NetworkPolicy with no controller "will have no effect"), compute (shared kernel and kubelet), or cluster-scoped objects (CRDs, webhooks). CoreDNS lists every service to every tenant. | [K8s multi-tenancy docs](https://kubernetes.io/docs/concepts/security/multi-tenancy/); [EKS guide](https://docs.aws.amazon.com/eks/latest/best-practices/tenant-isolation.html): "Kubernetes is a *single tenant orchestrator*"; [NetworkPolicy docs](https://kubernetes.io/docs/concepts/services-networking/network-policies/); SIG-Network built AdminNetworkPolicy because NetworkPolicy was "designed exclusively for… the Application Developer". **Strong.** | Non-goal, stated first in the docs, proto and CLI help. A plane-by-plane table says what a namespace does and does not do (§ 8.6). |
| K2 | **Namespaces stuck in `Terminating` forever.** One broken finalizer or unavailable aggregated API wedges deletion. The fix is hand-editing `/finalize`, which maintainers ask people not to do. | [#60807](https://github.com/kubernetes/kubernetes/issues/60807) (119 👍, 180 comments, locked); [#77086](https://github.com/kubernetes/kubernetes/issues/77086) closed "intended"; [GKE troubleshooting](https://docs.cloud.google.com/kubernetes-engine/docs/troubleshooting/terminating-namespaces). At Datadog a deleted namespace kept running for four days, then vanished when `metrics.k8s.io` recovered ([KubeCon NA 2020](https://raw.githack.com/sbueringer/kubecon-slides/master/slides/2020-kubecon-na/How%20the%20OOM-Killer%20Deleted%20My%20Namespace,%20and%20Other%20Kubernetes%20Tales%20-%20Laurent%20Bernaille,%20Datadog%20-How%20the%20oom-killer%20deleted%20my%20namespace.pdf)). **Strong.** | No `Terminating` state and no finalizers. Deleting a non-empty namespace is refused. `--cascade` runs synchronously and depends on no optional component (§ 8.11). |
| K3 | **You can't see why deletion is stuck, or what is in a namespace.** | [#70916](https://github.com/kubernetes/kubernetes/issues/70916) (open since 2018); [kubectl#151](https://github.com/kubernetes/kubectl/issues/151): `get all` isn't all (101 👍). **Strong.** | The refusal names what blocks it, counted by kind. `--dry-run` lists every object (§ 8.11). |
| K4 | **Deletion order is undefined, so pods can briefly lose their NetworkPolicy** (CVE-2024-7598). | [#126587](https://github.com/kubernetes/kubernetes/issues/126587). **Strong.** | A cascade has a fixed order. Net policies are cluster objects, not namespace contents, so a cascade never removes the policy protecting a task that is still running (§ 8.11). |
| K5 | **You can't list the namespaces you have access to.** | [#48537](https://github.com/kubernetes/kubernetes/issues/48537): "an ACL-filtered list API is a very different type of API". [#71097](https://github.com/kubernetes/kubernetes/issues/71097). OpenShift Projects exist partly to fix this ([OKD](https://docs.okd.io/4.9/rest_api/project_apis/project-apis-index.html)). **Medium-strong.** | `ListNamespaces` filters per item, the pattern every Legion list already uses. `WhoAmI` returns readable namespaces (§ 8.5). |
| K6 | **No pattern grants.** "Admin on every `foo-*` namespace" needs one RoleBinding per namespace. Label-selector RBAC was declined because labels are "mutable attributes of objects". | [Forum](https://discuss.kubernetes.io/t/kubernetes-rbac-rolebinding-for-multiple-namespaces/10273) (unanswered); [#44703](https://github.com/kubernetes/kubernetes/issues/44703). **Strong.** | Globs on immutable namespace names, which the engine already has. Never labels (§ 7 B, § 8.5). |
| K7 | **No deny rules,** so "everything except `kube-system`" can't be written. Wildcards grant "all object types which are created in the future". | [#85963](https://github.com/kubernetes/kubernetes/issues/85963) (99 👍); [RBAC good practices](https://kubernetes.io/docs/concepts/security/rbac-good-practices/). **Strong.** | Legion already has deny rules that override allows. "All namespaces" must be written as an explicit `["*"]`, never implied by leaving the selector out (§ 8.5). |
| K8 | **Role/ClusterRole × RoleBinding/ClusterRoleBinding confuses people.** Three popular tools exist only to answer "who can do what, where". | [RBAC docs](https://kubernetes.io/docs/reference/access-authn-authz/rbac/); rakkess 1.4k★, rbac-lookup 983★, kubectl-who-can 922★. **Medium.** | One policy kind. A namespace is one selector on a rule (§ 8.5). |
| K9 | **Rights inside a namespace escalate.** Whoever may create pods gets every service account's powers. Whoever may label their namespace can loosen Pod Security and NetworkPolicy. A service account can delete its own namespace ("I don't see anything to fix"). | [RBAC good practices](https://kubernetes.io/docs/concepts/security/rbac-good-practices/); [#134929](https://github.com/kubernetes/kubernetes/issues/134929); [KEP-2161](https://www.kubernetes.dev/resources/keps/2161/) added an immutable name label because the other labels are untrustworthy. **Strong.** | The namespace object carries nothing policy-bearing. Governance lives in objects a namespace writer cannot edit (§ 8.12). A cascade also needs `job:write` (§ 8.11). |
| K10 | **Cross-namespace references create confused deputies.** Ingress can't route across namespaces (liggitt: "a prime source of privilege escalation"). Gateway API had to invent ReferenceGrant. | [CVE-2021-25740](https://discuss.kubernetes.io/t/security-advisory-cve-2021-25740-endpoint-endpointslice-permissions-allow-cross-namespace-forwarding/16700); [#17088](https://github.com/kubernetes/kubernetes/issues/17088) (106 👍); [ReferenceGrant](https://gateway-api.sigs.k8s.io/reference/api-types/referencegrant/). **Strong.** | Crossing is explicit at both ends: the consumer names the namespace, and the target's `allow` names the consumer (§ 8.7). |
| K11 | **Secrets can't be shared across namespaces,** so copy-controllers proliferate. | [#70147](https://github.com/kubernetes/kubernetes/issues/70147) ClusterSecret (211 👍, closed unbuilt); reflector 1.7k★, replicator 1.1k★. **Strong.** | KV stays path-scoped, so sharing a path across namespaces is just a `paths` rule (§ 5, call 25). |
| K12 | **Quota is brittle.** It forces every pod to set requests, LimitRange backfills are non-deterministic, quotas are per namespace only, and the single quota object is a write hot spot. | [Quota docs](https://kubernetes.io/docs/concepts/policy/resource-quotas/); [LimitRange docs](https://kubernetes.io/docs/concepts/policy/limit-range/); [#67761](https://github.com/kubernetes/kubernetes/issues/67761) (409s on the quota object); OpenShift's ClusterResourceQuota, Rancher project limits, Kiosk AccountQuota and HNC HierarchicalResourceQuota all add cross-namespace totals. **Strong.** | Every Legion task already has a reservation (`taskResourceDefaults`, `docs/resource-enforcement.md:119`). Quota is an aggregate over a glob of namespaces, with counters rather than one hot row (§ 8.12). |
| K13 | **Flat namespaces don't model organizations, and the hierarchy fixes died.** HNC was archived in April 2025. WG Multitenancy was archived in 2023. Kiosk was archived. Capsule and vcluster carry on. | [HNC repo](https://github.com/kubernetes-retired/hierarchical-namespaces); [community#7362](https://github.com/kubernetes/community/pull/7362); [Kiosk](https://github.com/loft-sh/kiosk); [Capsule](https://projectcapsule.dev/docs/overview/): "they have no hierarchy". **Strong.** | Flat names, plus globs and aggregate namespace policies. That is the one grouping level OpenShift, Rancher and Capsule each converged on, without a nesting model (§ 5). |
| K14 | **Switching namespaces is painful, the current one is invisible, and `default` is a trap.** kubens has 20.0k★ and kube-ps1 3.8k★. In one real incident, an engineer deleted a prod namespace from the wrong context with "no contextual clues". | [kubectx](https://github.com/ahmetb/kubectx); [2i2c incident](https://2i2c.org/incident-reports/2025-08-12-leap-outage); [GKE best practices](https://cloud.google.com/blog/products/containers-kubernetes/kubernetes-best-practices-organizing-with-namespaces). **Strong.** | One env var, no persisted context, and every mutating command prints the qualified id it touched (§ 8.3, call 27). |
| K15 | **Errors didn't say which namespace was searched** (until 2024), and you can't query a chosen set of namespaces. | [kubectl#1459](https://github.com/kubernetes/kubectl/issues/1459); [#52326](https://github.com/kubernetes/kubernetes/issues/52326) `-n a,b` (18 👍). **Medium.** | Every not-found names the namespace. List commands take names or globs, comma-separated (§ 8.3). |
| K16 | **`-n` is silently ignored on cluster-scoped resources:** `get pv -n x \| delete` deletes every PV. | [kubectl#1413](https://github.com/kubernetes/kubectl/issues/1413); [SIG-CLI thread](https://groups.google.com/g/kubernetes-dev/c/y4Q20V3dyOk/m/vNbttyY-BgAJ). **Medium-strong.** | Cluster-scoped commands have no `-n` flag, so clap rejects it (§ 8.3). |
| K17 | **No rename, no move.** | [#43867](https://github.com/kubernetes/kubernetes/issues/43867) (56 👍): "pretty hard, so I can't see us doing it". **Strong.** | The same, decided up front with the reason written down (call 17). |
| K18 | **Name limits bite in other layers.** Namespace plus pod name over 217 characters fails at the kubelet. A namespace named like a TLD shadows public DNS. | [#104288](https://github.com/kubernetes/kubernetes/issues/104288); [namespaces docs](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/). **Medium.** | Namespace names are DNS labels (≤ 63 characters). Legion's resolver answers only under `svc.legion`, so there is nothing to shadow. A longest-name test covers every path that embeds the qualified id (§ 13). |
| K19 | **Every namespace needs its own copy of policy objects:** RoleBindings, a default-deny NetworkPolicy, quota, LimitRange, a default ServiceAccount. Tools exist mainly to stamp these out. | [Capsule](https://projectcapsule.dev/docs/overview/); [SIG-Scalability thresholds](https://github.com/kubernetes/community/blob/master/sig-scalability/configs-and-limits/thresholds.md). **Medium.** | A new namespace is one row. Policies are cluster documents that *select* namespaces by glob, so nothing is created per namespace. |

**Keep:** namespaces are cheap to create. Scoped-by-default with an explicit
way out (short names resolve in your own namespace; `-A` crosses). An
immutable name for policy to select on (KEP-2161). The namespace as the unit
of cost reporting.

### 3.2 Nomad

**The model.** Namespaces are first-class objects
(`nomad namespace apply`). They were Enterprise-only from 0.7 (2017) until
1.0 (December 2020). Jobs, allocations, evaluations, deployments, variables,
CSI and dynamic host volumes, native services and scaling policies are
namespaced. Nodes, node pools, ACL objects and quota specs are not. ACL
policies hold `namespace "dev-*" { … }` blocks, and the most specific glob
wins. The namespace object carries `meta`, driver and network-mode
allowlists, and workload-identity extra claims in the open-source edition.
Quotas, `node_pool_config` and Consul/Vault cluster allowlists are
Enterprise. The flag is `-namespace`, the env var is `NOMAD_NAMESPACE`, and
`*` crosses namespaces. Deleting a non-empty namespace is refused.

| # | Complaint | Evidence | Legion's answer |
| --- | --- | --- | --- |
| N1 | **Open-source namespaces are "just ACL scoping"; the governance is paid.** Users cite the paywalled multi-tenancy features as a reason to leave. A maintainer-run meta issue lists what "truly multi-tenant" still needs: nesting, delegated admins, per-namespace priority bounds, scheduler isolation. | [Namespace spec](https://developer.hashicorp.com/nomad/docs/other-specifications/namespace) (edition markers); HN [36857168](https://news.ycombinator.com/item?id=36857168), [27764627](https://news.ycombinator.com/item?id=27764627); [#18803](https://github.com/hashicorp/nomad/issues/18803) (open, "Needs Roadmapping"). **Strong.** | Governance ships in core (Phase 4), and the design leaves room for it from Phase 1. Delegated namespace administration is in Phase 1. Scheduler isolation is a non-goal (§ 5). |
| N2 | **Objects can be created in a namespace that doesn't exist.** A stray `NOMAD_NAMESPACE` landed Terraform jobs in a "nonexistent namespace". | [#15379](https://github.com/hashicorp/nomad/issues/15379); [terraform-provider-nomad#267](https://github.com/hashicorp/terraform-provider-nomad/issues/267); [Discuss](https://discuss.hashicorp.com/t/job-is-in-nonexistent-namespace-isolation/40165) (634 views). **Strong.** | Writes into a missing namespace are refused in the handler, with the namespace named (§ 8.1). |
| N3 | **"My jobs are gone."** The CLI defaults to `default`, and a token without `default` access gets a 403 from plain `nomad status`. A request to make `*` the default was declined because of the risk of accidental actions. | [Discuss 43052](https://discuss.hashicorp.com/t/nomad-cli-doesnt-show-running-jobs/43052) (1,248 views); [#11487](https://github.com/hashicorp/nomad/issues/11487). **Strong.** | `default` stays the default. The empty-list and not-found messages name the namespace and count what is hidden elsewhere, but only for namespaces the caller can read (§ 8.3). |
| N4 | **Wildcard `*` support arrived one command at a time over about six years.** Read-job with `*` still returns 404 by design. | Changelog: 0.12.0 → 2.0.7; [#18831](https://github.com/hashicorp/nomad/issues/18831): `service info` "is missing the logic we do for other 'get' commands"; [#10667](https://github.com/hashicorp/nomad/issues/10667), [#7341](https://github.com/hashicorp/nomad/issues/7341) (open). **Strong.** | One shared resolver for namespace selection, and a test that parses the proto and fails if any list RPC lacks namespace selection (§ 13). |
| N5 | **Jobspec `namespace` vs `-namespace` vs env var has no stated precedence.** That produced bugs and a CVE: CSI volumes created in unauthorized namespaces by putting the target namespace in the spec. | [`job run` docs](https://developer.hashicorp.com/nomad/commands/job/run); [#24039](https://github.com/hashicorp/nomad/issues/24039); [HCSEC-2024-27 / CVE-2024-10975](https://discuss.hashicorp.com/t/hcsec-2024-27-nomad-vulnerable-to-cross-namespace-volume-creation-abusing-csi-write-permission/71212). **Strong.** | A conflict is an error, enforced **server-side**, and authorization runs against the resolved namespace (§ 8.4, call 14). |
| N6 | **The event stream's wildcard bypassed namespace ACLs** (CVE-2025-0937, 1.0.0 through 1.9.5). Earlier, allocation reads leaked across namespaces (CVE-2019-16742). | [HCSEC-2025-02](https://discuss.hashicorp.com/t/hcsec-2025-02-nomad-vulnerable-to-event-stream-namespace-acl-policy-bypass-through-wildcard-namespace/73191); changelog. **Strong.** | `*` is never a separate authorization path. It widens the candidate set, and every item still passes the per-item filter. There is a test with a one-namespace token (§ 8.9, § 13). |
| N7 | **The UI kept losing the namespace:** 404 on reload, exec windows (fixed three times), dispatch "Not Found". The all-namespaces default made the UI slow. | [#12022](https://github.com/hashicorp/nomad/issues/12022); [#11970](https://github.com/hashicorp/nomad/issues/11970) ("an unintended side-effect"); [Discuss 29028](https://discuss.hashicorp.com/t/when-executing-parameterized-job-with-namespace-applied-in-ui-not-found/29028); changelog. **Strong** (many issues, few reactions each). | The namespace is a path segment in every detail route, so navigation can't drop it. The All view paginates (§ 10). |
| N8 | **One-namespace tokens break UI flows:** greyed-out buttons, an empty topology page. | [#23905](https://github.com/hashicorp/nomad/issues/23905); [#24334](https://github.com/hashicorp/nomad/issues/24334). **Strong.** | A one-namespace identity is a first-class UI test fixture (§ 13). |
| N9 | **ACL glob precedence is "most specific wins", and the docs had it wrong.** Malformed blocks were silently accepted (CVE-2023-3072: an unlabeled block applied to a namespace literally named `policy`). | [ACL policy spec](https://developer.hashicorp.com/nomad/docs/other-specifications/acl-policy); [#20118](https://github.com/hashicorp/nomad/issues/20118); [#17908](https://github.com/hashicorp/nomad/issues/17908). **Strong.** | No precedence: the union of allows, with any matching deny winning (`src/structs/acl.rs:445-470`). Rules already `deny_unknown_fields` (`:119`). A rule that omits or misplaces `namespaces` is rejected (§ 8.5). |
| N10 | **Only a management token can create namespaces.** | [#14003](https://github.com/hashicorp/nomad/issues/14003) (open, 5 👍); item 2 of [#18803](https://github.com/hashicorp/nomad/issues/18803). **Strong.** | A `namespace` ACL resource, delegable by glob (§ 8.5, call 12). |
| N11 | **Workload-identity policies could only attach per job until 1.10.2.** The implicit policy lets any workload read every service registration ("seeing other teams' jobs can be a security issue"). | [#17181](https://github.com/hashicorp/nomad/issues/17181); [#19803](https://github.com/hashicorp/nomad/issues/19803). **Strong.** | Workload binding rules can match the namespace claim from Phase 1 (§ 8.8). |
| N12 | **Delete safety was added piecemeal and kept breaking.** Deletion was allowed with CSI volumes present. A delete with variables returned 200 and failed in the FSM. Quota↔namespace cycles broke Terraform teardown. | [#19414](https://github.com/hashicorp/nomad/issues/19414); [#23672](https://github.com/hashicorp/nomad/issues/23672); changelog GH-13880, GH-23499. **Strong.** | One emptiness check over a single list of namespaced kinds, run in the same store write as the delete. Namespace policies select namespaces by glob, so there is no cycle (§ 8.11). |
| N13 | **Client-side keys and paths lacked the namespace.** Same-id volumes in two namespaces failed to stage; the CSI expand path omitted the namespace. | Changelog GH-20532 (1.8.0), GH-25253 (1.9.8). **Strong.** | The qualified id is the storage key, so every key and path derived from a job id carries the namespace by construction (§ 8.2, call 1). |
| N14 | **On Consul CE, same-named services from two Nomad namespaces merge into one,** and Enterprise maps namespaces through five levels of precedence. | [Discuss 16712](https://discuss.hashicorp.com/t/nomad-oss-namespace-and-service-discovery/16712): "a single service in consul with 2 instances"; [#14810](https://github.com/hashicorp/nomad/issues/14810). **Strong.** | One namespace concept across every subsystem, with no mapping fields. Services are namespaced in the same phase as jobs (§ 8.7, call 6). |
| N15 | **Cross-namespace service lookup is the most-upvoted namespace ask.** | [#14177](https://github.com/hashicorp/nomad/issues/14177) (open since 2022, 7 👍 9 ❤️): "I can't even begin this migration until template blocks are able to cross-resolve services"; [PR #28534](https://github.com/hashicorp/nomad/pull/28534); [HN](https://news.ycombinator.com/item?id=43817851). **Strong.** | Crossing is designed in from Phase 1 and shipped in Phase 2, explicit at both ends (§ 8.7). |
| N16 | **Nothing can be shared across namespaces:** "No object in Nomad can be created with a wildcard namespace identifier." | [#28455](https://github.com/hashicorp/nomad/issues/28455) (closed, not planned); [Discuss 57557](https://discuss.hashicorp.com/t/create-global-variables-used-by-all-namespaces/57557). **Strong.** | KV and job templates stay cluster-scoped, so sharing them is the default and is governed by ACL selectors (§ 8.10, calls 20 and 25). |
| N17 | **There is no namespace topic in the event stream;** users poll. | [#10887](https://github.com/hashicorp/nomad/issues/10887). **Medium.** | Namespace lifecycle events (§ 8.9). |
| N18 | **The namespace label is inconsistent across metrics,** and chargeback is do-it-yourself. | [#8638](https://github.com/hashicorp/nomad/issues/8638) (open since 2020); [Discuss 59870](https://discuss.hashicorp.com/t/is-there-any-way-to-calculate-the-cost-of-nomad-resources-per-namespace-per-job-etc/59870). **Medium.** | A `namespace` label on every workload family, plus a usage family per namespace (§ 8.9). |
| N19 | **Node pools (1.6) had to be added because namespaces never controlled placement.** Before them, people abused datacenters. Namespace→pool rules are Enterprise. | [#11041](https://github.com/hashicorp/nomad/issues/11041): "I'm not a fan of (ab)using the datacenter for this purpose"; [1.6 blog](https://www.hashicorp.com/blog/nomad-1-6-adds-node-pools-ux-updates-and-more). **Strong.** | Legion has pools already. Allowed and default pools per namespace are Phase 4, checked at submit. |
| N20 | **The driver allowlist went on the namespace, not into ACLs**, to avoid a "combinatorial explosion of capabilities". | [#11807](https://github.com/hashicorp/nomad/issues/11807). **Strong.** | Allowed runtimes are a namespace policy, not an ACL selector (§ 8.12). |
| N21 | **No rename, no move.** | [Discuss 51981](https://discuss.hashicorp.com/t/namespace-renaming/51981) (unanswered). **Weak-moderate.** | The same, with the reason written down (call 17). |
| N22 | **IDs are global while names are per namespace.** Eval GET ignores the namespace you pass. | [#12919](https://github.com/hashicorp/nomad/issues/12919); [Discuss 67215](https://discuss.hashicorp.com/t/are-ids-unique-accross-all-namespaces/67215). **Moderate.** | Numeric ids (allocations, rollouts) stay global. Every lookup by id runs the coarse-then-precise check against the owning job's namespace (§ 8.5). |

**Keep:** the name guarantee ("job names will not conflict"). Glob-matched
namespace ACLs. `*` plus an env var. The driver allowlist on the namespace.
The namespace claim in workload identity. And the celebration when it went
open source, from users who had otherwise "just [chosen] kubernetes because
isolating containers was not possible" ([HN](https://news.ycombinator.com/item?id=25358533)).

### 3.3 Everyone else

| System | Model | Lesson | What Legion takes |
| --- | --- | --- | --- |
| **Consul** | Namespaces (1.7), then admin partitions (1.11) one level above, because "resource nodes were not namespaced prior to 1.11" ([1.11](https://developer.hashicorp.com/consul/docs/v1.11.x/enterprise/admin-partitions)). | A naming boundary gets asked to do isolation work it can't do, and a second tier follows. | State the isolation non-goal up front. Pools are the compute tier, and federation is the hard tier. |
| **Vault** | Hierarchical `A/B/C` namespaces with their own auth, policies and tokens ([docs](https://developer.hashicorp.com/vault/docs/enterprise/namespaces)). | "Use them sparingly." Deletion leaked dynamic secrets ([HCSEC-2020-03](https://discuss.hashicorp.com/t/hcsec-2020-03-vault-enterprise-s-dynamic-secrets-may-persist-after-namespace-deletion/18086)). There were cross-namespace policy CVEs (CVE-2023-3775), and a 2026 path-traversal CVE in the policy cache (CVE-2026-105820). | Flat names. Authorization works on structured `(namespace, name)` fields and never parses a path (§ 8.2). |
| **OpenShift** | A Project is a namespace plus annotations. Users create one through a templated `ProjectRequest`; `ProjectRequestLimit` capped projects per user ([OKD](https://docs.okd.io/4.20/applications/projects/configuring-project-creation.html)). | Self-service creation wants a template and a limit, not raw create rights. | Delegated creation by glob (Phase 1). A namespace count cap per policy (Phase 4). Templated creation is open question 7. |
| **Rancher** | Projects group namespaces; permissions and quotas are inherited ([docs](https://ranchermanager.docs.rancher.com/how-to-guides/advanced-user-guides/manage-projects/manage-project-resource-quotas/about-project-resource-quotas)). | Changing a default doesn't reach existing namespaces. Membership-by-annotation made kubectl-created namespaces "unusable". | Grouping is a glob over immutable names, evaluated live, so nothing has to be backfilled. |
| **Cloud Foundry** | Exactly two levels, org and space. Every org must have a quota; space quotas are optional. Deploy checks space, then org ([docs](https://docs.cloudfoundry.org/adminguide/quota-plans.html)). | Two quota levels that both apply. | Several namespace policies can match one namespace, and all of them apply (§ 8.12). |
| **GCP / AWS / Azure** | Project IDs are "permanent", never reused. SCPs "do not grant permissions"; they set a maximum. Tag-based authorization needs explicit denies on tag edits. Resource groups can't be renamed ([GCP](https://docs.cloud.google.com/resource-manager/docs/creating-managing-projects), [AWS SCP](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_scps.html), [Azure](https://docs.azure.cn/en-us/azure-resource-manager/management/frequently-asked-questions)). | Decide on name permanence up front. Never authorize on attributes the subject can edit. | No rename. Name reuse is allowed but visible (§ 8.11). No label-based authorization (§ 7 B). |
| **Borg** | `cell/user/job`. Quota is a resource vector "at a given priority" per user per cell. "Quota-checking is part of admission control, not scheduling." "Every user has infinite quota at priority zero" ([paper](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/43438.pdf)). | Quota per priority band, checked at admission. | Phase 4 quotas are per band and checked at submit. `background` is uncapped unless named (§ 8.12). |
| **Twine (Meta)** | Entitlements were "overloaded… with two responsibilities: fleet partitioning and quota management" ([OSDI 2020](https://www.usenix.org/system/files/osdi20-tang.pdf) §7.1), and had to be split. | Don't make the tenancy unit a slice of the fleet. | A namespace is not a scheduling input. Pools partition the fleet; namespace policies only *allow* pools. |
| **Temporal** | A namespace is the unit of isolation and retention. "You must create a Namespace before using it", including `default`. Cloud names can't be changed ([docs](https://docs.temporal.io/namespaces)). | A missing built-in default is friction. | `default` exists and can't be deleted. |
| **Pulsar** | `tenant/namespace/topic`. A namespace's replication clusters must be a subset of the tenant's allowed clusters ([PIP-321](https://apache.googlesource.com/pulsar/+/HEAD/pip/pip-321.md)). | Allowlists that narrow, never widen. | Allowlists from several matching policies intersect (§ 8.12). |
| **Kafka** | No namespaces: prefixed ACLs ([KIP-290](https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs)). | A prefix match needs a delimiter (`team-a` also matches `team-ab`). | The docs tell admins to glob with the delimiter (`payments-*`, not `payments*`). |
| **Docker Swarm, Fly.io** | Stacks are a name prefix plus a label. Fly app names are global and org slugs "can't be changed". | The status quo made official; users dislike it. | Option A in § 7, rejected. |

---

## 4. Jobs to be done, by persona

**Security / platform admin**

- When I give a team access to the shared cluster, I want to give them one
  name that means "their stuff", so I can write a permission rule once
  instead of maintaining a list of prefixes that grows every time Legion
  grows a feature.
- When I let a team lead create their own environments, I want their total
  use capped no matter how many environments they create, so self-service
  doesn't become a way around the limit.
- When I add a new team, I want to know nobody else's permissions
  accidentally reach into their work, so I don't have to audit every rule to
  find out who just got access.
- When someone leaves and I revoke their access, I want to see exactly what
  they could reach, so I can answer "what was exposed" honestly.

**Cluster operator**

- When I look at a cluster several teams share, I want to see whose work is
  whose at a glance, so I know which team to call when something is wedged.
- When a team is done with an environment, I want to remove it and
  everything in it in one step, without leaving orphans and without the
  removal getting stuck half-done.
- When one team is filling the cluster, I want a way to cap what they can
  take, so one team's bad afternoon isn't everyone's.

**Job author / app developer**

- When I name my service, I want to call it what it is (`web`, `api`,
  `worker`) without checking whether another team took the name.
- When my service talks to `redis`, I want it to reach *my* redis, not
  another team's that happens to share the name.
- When I copy a deployment from staging to production, I want the only
  difference to be where it lands, not a renamed job and a rewritten set of
  dashboards.

**On-call responder**

- When I'm paged about a service, I want the thing I'm looking at to say
  which team owns it, so I know whether I can restart it or need to wake
  someone.
- When I paste a link into the incident channel, I want the other person to
  see the same thing I see, not the same name in their own environment.
- When I have permission for one team's environment and not another's, I
  want the tools to tell me that clearly, not fail on the fifth command.

**Autonomous agent**

- When I'm given credentials for one team, I want every list I read and
  every action I take to stay inside that team without remembering a flag,
  so I can't damage anything outside my remit even if I misparse a name.
- When I report what I did, I want the identifier I print to be unambiguous
  cluster-wide, so a human can paste it back and find the same thing.

*Out of scope:* a tenant who needs hard isolation from a hostile neighbour.
That tenant gets a node pool and `sandbox` today, and a separate cluster under
federation later. § 8.6 says which, and the public page says it first.

### User scenarios the design must serve

1. **Two teams, same job name.** Payments runs `web` and checkout runs
   `web`. Neither can see, stop, exec into, scale or *route to* the other's.
   An operator sees both in one view, told apart by namespace.
2. **A tenant token that can't escape.** A token scoped to `payments-*`
   lists jobs, reads events, resolves services and opens exec sessions. Each
   of them is filtered to `payments-*`, with no flag involved and no way to
   opt out. A wildcard request (`-A`) returns the same filtered set.
3. **A delegated environment stays inside the budget.** The payments lead
   runs `legion namespace create payments-loadtest`, submits a large job,
   and is stopped at payments' aggregate quota. Payments' prod and staging
   don't lose their headroom (Phase 4).
4. **Retiring an environment.** `legion namespace delete payments-old`
   refuses and lists what is in it. `--cascade` drains it in one re-runnable
   command, with no wedged half-deleted state.
5. **An agent with a one-namespace token** runs `job run`, `job status` and
   `exec`. Every reply carries the qualified id (`payments-prod/web`), and a
   mistaken attempt on `checkout-prod/web` gets `PermissionDenied` naming the
   namespaces it *can* write.

---

## 5. Goals and non-goals

### Goals

1. A first-class `Namespace` object with CRUD over gRPC, CLI, MCP and UI.
   Creating namespaces can be delegated by glob.
2. Job identity becomes `(namespace, name)`, and **everything keyed by a job
   inherits the namespace by construction**. That covers versions,
   instances, volumes, snapshots, tickets, seal keys, content-store paths,
   events and identity subjects.
3. ACL rules scope by namespace through the **existing** `namespaces`
   selector. No new policy kind, no second engine. The reach of every rule
   is written explicitly.
4. **One namespace concept across every subsystem:** jobs, services and
   mesh DNS, workload identity, events, volumes. No per-integration mapping
   fields (N14).
5. Crossing a namespace boundary is always explicit and visible at both
   ends.
6. Namespace-aware ergonomics wherever a human types: `-n`,
   `LEGION_NAMESPACE`, `-A`, `namespace/name`, and errors that name the
   namespace.
7. **A single-team cluster never notices.** `default` exists and can't be
   deleted. One namespace means no tenancy UI and no flags.
8. Governance in the open product, shaped so that delegated creation can't
   dodge it.

### Non-goals

- **Namespaces are not an isolation boundary.** No network isolation, no
  kernel isolation, no implicit node separation (K1, Consul, N19). Compute
  isolation is node pools. Network isolation is `network.mode = "gated"`
  plus net policies ([docs/network-handlers.md](../../docs/network-handlers.md)).
  Hostile-tenant isolation is the `sandbox` runtime or, later, federation.
  A namespace never implies a network mode or a pool, because a tenancy
  boundary that silently rewires a workload breaks deploys.
- **No hierarchy.** `team/prod` as a nested object is out. HNC was retired,
  Kiosk was archived, and Vault says "sparingly" (K13). Globs over flat names
  (`payments-*`) plus aggregate namespace policies cover the cases we can
  name.
- **No per-namespace RBAC objects.** No namespaced policies, no
  `Role`/`ClusterRole` split (K8). Policies stay cluster documents that
  *select* namespaces.
- **No rename, no move** (K17, N21). Call 17 gives the reason.
- **No finalizers, no `Terminating`** (K2). Deletion is refused or done
  synchronously.
- **No namespaced KV.** KV keeps its `paths` scoping (K11, N16, call 25).
- **Not a scheduling input.** Placement is unchanged in every phase. Phase 4
  checks allowed pools and quota at *admission* (Borg), and never in
  placement or scoring.
- **No per-namespace scheduler isolation** (item 4 of N1). Eval queues stay
  shared, and priority bands govern contention.
- **No migration of pre-namespace data directories** (call 18).
- **No persisted CLI namespace context.** There is no `namespace use`
  (call 27).
- **No tenant-authored net policies.** Net policies stay platform objects;
  their namespace selectors start matching real values in Phase 1, and
  delegating authorship is deferred (§ 8.7).

---

## 6. What exists today

### 6.1 The namespace placeholder

| Piece | Location | State |
| --- | --- | --- |
| `namespaces` rule selector | `src/structs/acl.rs:129-131` | Real, glob-matched, unit-tested (`:732-747`). The doc comment says "jobs live in `default` today". |
| `AclScope.namespace` | `src/structs/acl.rs:211` | Real field |
| `DEFAULT_NAMESPACE = "default"` | `src/structs/acl.rs:235`, and a **second** copy at `src/structs/netpolicy.rs:116` | The only value ever produced |
| Constructors that stamp it | `AclScope::job` `:252`, `::exec` `:277`, `::action` `:287`, `::job_via_template` `:305` | All hardcoded. `pool`, `path`, `template`, `none` and `any` carry no namespace. |
| Namespace matching | `rule_matches`, `src/structs/acl.rs:430` | Real |
| UI mirror | `ui/src/utils/acl.ts:61` (`DEFAULT_NAMESPACE`), `jobScope` `:64-68` (16 call sites) | Same hardcode. The mirror has also **drifted** from Rust: no `boot` resource, no `bands`, and the `host` check covers only `exec`. |
| Audit log `namespace` | `src/manager/acl.rs:780` | Always `default` or `-` |
| `NetSelector.namespaces` | `src/structs/netpolicy.rs:132-135`, matched at `:167-195` | Real glob list. All 7 builders of `NetSelectorFacts` pass `namespace: String::new()`, which is read as `default` (`store.rs:13233, :13277, :13423`; `server.rs:2497, :12960, :13055, :13328`). |
| `net` ACL scope | `enforce_net_policy_scope`, `src/manager/server.rs:13518-13530` | Pools only. A `net` rule with `namespaces` matches nothing. |
| Workload identity subject | `DEFAULT_SUB_TEMPLATE = "{region}:{job}:{group}:{task}"`, `src/structs/workload_identity.rs:90` | Documented as "Nomad's composed subject minus the namespace segment Legion lacks" (`:87-89`). There is no namespace claim (`:636-655`). |
| Jobspec `namespace` field, `Namespace` object, RPC, CLI | — | Do not exist |

### 6.2 The code the design has to fit

- **A job's id *is* its name.** Submit inserts `":id": config.name,
  ":name": config.name` (`src/storage/store.rs:10545-10547`), and the
  scheduler says so outright: "(job id == job name)"
  (`src/manager/scheduler.rs:1665`). Uniqueness comes from the primary key on
  `job.id` (`src/storage/migrations.rs:101-109`). **So every `job_id` column
  in the schema stores the bare name.** The first version of this plan said
  the foreign-key graph would not need to change. It does: either every
  job-keyed column changes, or what goes into `job.id` changes. § 8.2 picks
  the second.
- **What is derived from the bare job id:**
  - `JobConfig::id()` is `"{name}-{version}"` (`src/structs/job.rs:2457`).
  - A slot instance id is `"{job}-{group}-{slot}"` (`src/structs/instance.rs:272`).
  - Volume claim ids are a hash of `job\x01group\x01task\x01name`
    (`src/storage/store.rs:20982`).
  - Snapshot aliases are keyed `(job_id, name)`.
  - Checkpoint seal keys and chunk keys are HKDF'd over the job id
    (`src/checkpoint_seal.rs:320-345, :481`).
  - Archive manifests live at `manifests/{job_id}/…`
    (`src/structs/archive.rs:30-41`), and session manifests at
    `sessions/{job_id}/…` (`src/session_manifest.rs:58`).
  - Boot, exec and session tickets carry a `job` claim.
  - `job_kv_reference` is keyed `(job_id, kv_key)`.
  - Scale rows are keyed `(job_id, task_group, task_name)`.
- **Job names are already strictly validated.** `validate_object_name`
  (`src/structs/job.rs:1498-1513`) allows `[A-Za-z0-9._-]`, 1–128
  characters, with no leading dot. **`/` and `:` are already illegal**, so
  `namespace/name` is unambiguous with no new validation and no
  grandfathering.
- **ACL enforcement is centralized and scope-shaped.** There are 109
  `check_acl` call sites across 136 Manager RPCs. 46 `AclScope::job(` sites
  in `src/manager` build a job scope, along with 3 `exec`, 3 `action` and 2
  `job_via_template` sites, plus `job_read_scope` (`server.rs:1998`, 9
  callers) and `volume_write_scope` (`:11780`). Changing the constructor
  signatures turns every missed site into a compile error.
- **List endpoints already gate coarse, then filter per item.** For
  example, `list_jobs` gates on `AclScope::any()`, then `retain`s on
  `AclScope::job(..)` (`server.rs:5444-5489`). Namespace filtering rides the
  same path.
- **Selector matching.** An omitted selector matches any value, including
  a missing attribute (`selector_matches`, `acl.rs:387-393`). A selector
  whose attribute the request lacks fails (`:391`). `parse_rules`
  (`:472-509`) validates resources, actions and effect, and **never checks
  selector values or applicability**.
- **Events are all-or-nothing.** `list_events` and `stream_events` gate on
  `operator:read` with `AclScope::none()` and filter nothing
  (`server.rs:4666-4703, :4984-5043`).
- **Idempotency is already scoped per caller and per operation.**
  `scoped_idempotency_key` builds
  `"{operation}/{len}:{caller}/{key}#{fingerprint}"` (`server.rs:11730-11763`),
  and a reused key with a different request fingerprint is `InvalidInput`
  (`store.rs:19653-19676`). The first version's "team B replays team A's
  `deploy-1`" leak was fixed separately. Namespaces need nothing here
  beyond the namespace being part of the fingerprinted request.
- **Services are a projection, not a table.** Endpoints come from
  `task_config.service` JSON. `ListServices` collapses duplicate names into
  one summary through a `BTreeMap` keyed by service name
  (`server.rs:10337-10373`). Mesh DNS is `<key>.svc.legion`
  (`src/structs/network.rs:22`). The node resolver **rejects multi-label
  names** (`src/worker/netd/dns.rs:406-426`, test at `:628`), so a
  `<svc>.<ns>.svc.legion` form needs a resolver change. `allow` intentions
  are bare job ids and "not cryptographic identity" (`network.rs:574-579`).
- **Instance ids are global.** `instance.instance_id` is the primary key
  (`migrations.rs:458-478`). The existence check is cluster-wide
  (`store.rs:16977-16986`), and a test asserts "ids are unique cluster-wide"
  (`server.rs:19254`).
- **Job templates are cluster objects** (`job_template.name` primary key,
  `migrations.rs:964`). `enforce_template_collision_guard`
  (`server.rs:2574-2607`) tells a template-only submitter whether *any* job
  of a given name exists, even one they can't read. That is a pre-existing
  existence oracle, now in [TODO.md](../TODO.md).
- **Node caches serve images without checking registry credentials.** A
  digest the node already holds is served "with no registry request", and a
  tag whose registry lookup fails, auth failures included, falls back to the
  node's last-resolved digest (`src/artifacts/fetchers/oci.rs:611-621, :701`).
  So a job with no credentials for a private image can run it if another job
  pulled it to the same node. This predates namespaces. Namespaces turn it
  into a tenant-to-tenant leak: it is the problem Kubernetes' `AlwaysPullImages`
  admission plugin exists for. Tracked in [TODO.md](../TODO.md), and named in
  the public page's limitations until it is fixed.
- **The CLI has no global flags.** `--json` is attached per command by
  `json_flag()` (`src/cmd/helpers.rs:385`, 131 uses). `--addr` and `--token`
  flags don't exist. The precedent is env, then file, then default
  (`LEGION_ADDR` `:124`, `LEGION_TOKEN` `:194`).
- **UI detail routes key on bare names:** `/jobs/:id/...`
  (`ui/src/App.tsx:1149-1152`), `/services/:name` (`:1155`), and
  `/volumes/:id` (`:1158`).
- **Workload-identity subjects already have a uniqueness floor.**
  `validate_sub_template` hard-rejects any template missing `{job}`,
  `{group}` or `{task}` (`src/structs/workload_identity.rs:543-551`). The
  namespace joins that floor (§ 8.8).

---

## 7. Alternatives compared

| # | Shape | Collision-free names | Policy anchor | Governance anchor | Cost |
| --- | --- | --- | --- | --- | --- |
| A | Naming convention only (today) | by convention | via `names` globs | no | none |
| B | Labels, plus label selectors in ACLs | no | yes, N-dimensional | weak | low |
| C | Namespace as an attribute; names stay globally unique | **no** | yes | yes | low |
| D | Namespace as a name prefix the user types (`team-a/web` *is* the name) | yes | yes | yes | medium |
| E | **Namespace as an object; identity is `(namespace, name)`** | **yes** | **yes** | **yes** | high |
| F | One cluster per tenant (federation) | yes | yes | yes | very high |

**A, convention only.** It is free, and a `names: ["team-a-*"]` rule really
does stop team A creating `team-b-web`. It can't display the tenancy, it
doesn't survive a new resource type, and it gives governance nowhere to
live. § 1 lists what it costs.

**B, labels.** Labels are multi-dimensional (team, env and tier at once),
which is attractive. They are rejected on security grounds: **a label is
asserted by the submitter**. An authorization rule that matches a mutable,
submitter-controlled attribute is only as strong as the admission control
that binds labels to identities.

- Kubernetes declined label-selector RBAC for exactly this reason (K6).
- AWS warns that tag-based authorization needs explicit denies on editing
  the tags.
- Docker UCP's label-based collections had uneven mutability across object
  types.

Labels remain a good idea for *organizing* things, and they are orthogonal
to this plan.

**C, attribute only.** Add a column, stamp it on the scope, filter lists.
That delivers the policy anchor, the governance anchor and the grouping. It
doesn't deliver the one property people actually ask for. The first version
of this plan shipped C as a stepping-stone phase, purely to keep a migration
invisible. With no installed base, that reason is gone (call 1).

**D, prefix as the name.** The namespace object exists, but identity is one
string the user types and every consumer parses. Rejected: the parse rule
becomes load-bearing in the UI, CLI, event stream and dashboards, and a name
like `team-a/web` makes it look as if the name itself is something you could
change. Docker Swarm stacks work this way, and users dislike it.

**E, chosen**, with one implementation detail that sounds like D and isn't.
The storage key for a job is the string `namespace/name` (§ 8.2). The API,
the authorization scope and every struct carry `namespace` and `name` as
separate fields, and nothing parses the key to recover the tenant.

**F, one cluster per tenant.** This is the only option that actually
isolates. It is already planned
([multi-cluster-federation-plan.md](multi-cluster-federation-plan.md)), and
its design assumes namespaces exist. It complements namespaces rather than
replacing them, at a higher price point: one control plane per tenant, and
the Borg numbers in B1.

---

## 8. Proposed design

### 8.1 The namespace object

A namespace is a small object that exists to be referred to.

```nickel
# namespaces/payments-prod.ncl
{
  name = "payments-prod",
  description = "Payments, production",
  meta = { owner = "payments", slack = "#payments-oncall" },
}
```

```console
$ legion namespace list
NAME            JOBS   DESCRIPTION              CREATED
default         3      Default namespace        2026-10-01
payments-prod   12     Payments, production     2026-10-18

$ legion namespace create payments-loadtest --description "Load tests"
$ legion namespace apply -f namespaces/payments-prod.ncl
$ legion namespace get payments-prod -j
$ legion namespace delete payments-loadtest [--dry-run] [--cascade]
```

Rules:

- **Name grammar.** A DNS label: lowercase letters, digits and `-`, starting
  and ending alphanumeric, at most **63** characters (the first version said
  64, which doesn't fit a DNS label; § 8.7). `default` always exists and
  can't be deleted. The `legion-` prefix is reserved for system namespaces,
  so a future built-in can't collide with a tenant's. `*` and `,` can't
  appear, so neither is ambiguous in a selector or on the command line.
- **Writes into a namespace that doesn't exist are refused, in the
  handler** (N2). This covers job submit, template run and volume create.
  The error names the namespace and says who can create it:

  ```console
  $ legion job run web.ncl -n paymnets-prod
  error: namespace 'paymnets-prod' does not exist
  hint: `legion namespace list` shows the namespaces you can read
  ```

  Implicit creation would mean a typo silently forks a tenant.
- **The object carries nothing that governs anything.** `description` and
  `meta` are free-form, and Legion never reads them (Nomad's `meta`,
  [#9631](https://github.com/hashicorp/nomad/issues/9631)). Quotas and
  allowlists live in namespace policies that a namespace writer can't edit
  (§ 8.12, K9). So holding `namespace:write` on your own namespace grants
  exactly the power to describe it, create siblings within your glob, and
  delete it.
- **No rename** (call 17).
- **Lifecycle is observable.** `namespace_created`, `namespace_updated` and
  `namespace_deleted` go on the event stream (N17).

### 8.2 Job identity: the qualified id

Job identity becomes `(namespace, name)`. The storage key, `job.id`, is the
**qualified id**, `"{namespace}/{name}"`, minted in one place at submit:

```text
job.id         = "payments-prod/web"     # storage key, and the display form
job.namespace  = "payments-prod"         # its own column
job.name       = "web"
job_config.id  = "payments-prod/web-4"   # "{job_id}-{version}", unchanged code
slot instance  = "payments-prod/web-frontend-0"
seal key input = HKDF(..., "payments-prod/web")
manifest path  = manifests/payments-prod/web/<group>/<task>/<instance>.json
```

That one change puts the namespace into every key derived from a job id: the
versions, slot instances, volume claim ids, snapshot aliases, tickets, KV
references, scale rows, seal and chunk keys, and content-store paths. This
is the class of bug Nomad fixed one key at a time (N13), closed by
construction. The alternative is threading a `namespace` column through
every job-keyed table and every derived-key function. That is about thirty
places, and any one that is missed is a cross-tenant collision.

The invariants that keep this from turning into option D:

- **Authorization never parses the id.** Scopes carry `namespace` and `name`
  as fields (`AclScope::job(namespace, name)`), set from the stored columns.
  Vault's path-traversal CVE in its policy cache is the warning
  (CVE-2026-105820).
- **The wire carries both fields.** Every reply that has a `job_id` also has
  `namespace` and `job_name` (§ 11).
- **Path builders split it.** Every site that today treats `job_id` as one
  safe path component (`is_safe_path_component`) takes two components
  instead. Phase 1 enumerates them with `rg`, and the longest-name test
  (§ 13) covers them.
- **Keyed instance ids are unique per namespace**, not cluster-wide. The
  instance primary key becomes the qualified `"{namespace}/{instance_id}"`.
  The "unique cluster-wide" test is inverted into "two namespaces may both
  hold `user-42`". That removes today's cross-tenant existence oracle.
  Fork ids (`<caller id>-f<n>`) stay in their parent's namespace.
- **Numeric ids stay global.** Allocations, rollouts and evals keep their
  autoincrement ids. A lookup by id gates coarse first, then enforces the
  owning job's `(namespace, name)` (AGENTS.md "check before you fetch";
  N22).

### 8.3 References, the CLI, and the active namespace

| Form | Meaning |
| --- | --- |
| `web` | `web` in the **active namespace** |
| `payments-prod/web` | `web` in `payments-prod`, whatever the active namespace is |
| `-n payments-prod web` | the same |

**The active namespace** is the `-n/--namespace` flag, else
`LEGION_NAMESPACE`, else `default`. That is the env-then-default precedent
`LEGION_ADDR` and `LEGION_TOKEN` set, with a flag in front because unlike
an address you switch namespaces mid-session.

- **`-n` is attached per command** by a `namespace_flag()` helper beside
  `json_flag()`, and only to commands that act on namespaced things.
  `legion node list -n prod` is a clap error, not a silently-ignored flag
  (K16).
- **List commands** take names or globs, comma-separated
  (`-n 'payments-*,shared'`). `-A/--all-namespaces` means `-n '*'`, and
  adds a `NAMESPACE` column. Every other command takes exactly one concrete
  namespace (K15).
- **A conflict is an error:**

  ```console
  $ legion job status checkout-prod/web -n payments-prod
  error: 'checkout-prod/web' names namespace 'checkout-prod' but -n says 'payments-prod'
  hint: drop -n; a qualified name does not need it
  ```

- **Not-found always names the namespace, and helps without leaking** (K15,
  N3):

  ```console
  $ legion job status api
  error: job 'api' not found in namespace 'default'
  hint: a job named 'api' exists in namespace 'payments-prod'; retry with -n payments-prod
  ```

  The hint appears only for namespaces where the caller holds `job:read`.
  Shown to anyone else, it would be an existence oracle.
- **An empty list says what it hid:**

  ```console
  $ legion job list
  No jobs in namespace 'default'. 14 jobs in 2 other namespaces you can read; use -A.
  ```

- **Permission-denied names where you *can* act:**

  ```console
  $ legion job stop checkout-prod/web
  error: permission denied: job:write on checkout-prod/web
  hint: this token can write jobs in payments-*
  ```

- **Every mutating command prints the qualified id it touched** ("Stopped
  payments-prod/web"). That is the "contextual clue" the 2i2c incident
  lacked (K14).
- **Tasks get `LEGION_NAMESPACE` in their environment**, alongside the job
  and task names. A CLI run inside a task therefore defaults to the task's
  own namespace, which is Kubernetes' in-cluster behavior, and a workload
  can label its own metrics.
- **There is no persisted context.** No `legion namespace use`, no
  `~/.legion/namespace` (call 27). `export LEGION_NAMESPACE=…` is the sticky
  form: it lives as long as the shell and shows up in `env`.

Agents get the same thing through MCP: every job tool takes an optional
`namespace`, accepts a qualified name, and returns qualified ids.

### 8.4 The jobspec surface

```nickel
{
  name = "api",
  namespace = "payments-prod",   # optional; defaults to the active namespace
  tasks = [ ],
}
```

This is one new optional string, and it goes into `configs/jobs/maximal.ncl`.

**The resolved namespace is decided server-side, and a conflict is
rejected server-side** (N5):

| Spec `namespace` | Request `namespace` | Result |
| --- | --- | --- |
| unset | unset | `default` |
| unset | `dev` | `dev` |
| `prod` | unset | `prod` |
| `prod` | `prod` | `prod` |
| `prod` | `dev` | **`InvalidArgument`**: "jobspec declares namespace 'prod' but the request names 'dev'" |

Authorization runs against the resolved namespace, after resolution. The
same rule covers every request that carries a spec: `RunJob`, `PlanJob`,
`ValidateJob`, `RunJobTemplate`, and volume create. It is a server rule,
not a CLI nicety, because the Nomad CVE was a client sending an authorized
namespace in the request and a different one in the spec.

### 8.5 The ACL model

**No new rule vocabulary.** The `namespaces` selector exists, globs, and
composes with `names`, `bands`, `templates`, `actionNames` and `host`. What
changes is that it now matches something real, and that a rule must say its
reach.

**Namespaced and cluster-scoped resources:**

| Namespaced (scope carries a namespace) | Cluster-scoped (scope never does) |
| --- | --- |
| `job`, `exec`, `action`, `scale`, `boot`, `snapshot`, `namespace` | `node`, `kv`, `state`, `operator`, `acl`, `template`, `net` |

`template` is cluster-scoped because templates are (call 20): a `template`
rule governs who may write the template *object*. Submitting *through* a
template is a `job` scope (`job_via_template`), so it is namespaced and
carries the target namespace. `net` is cluster-scoped because net policies
are. `net show` and `net test` against a job additionally check `job:read`
in that job's namespace, instead of building a job scope under the `net`
resource (`server.rs:9727, :9775`).

**A rule over a namespaced resource must carry `namespaces`. A rule over
only cluster-scoped resources must not** (call 3). Both are checked in
`parse_rules`, at `legion acl policy apply`:

```console
$ legion acl policy apply ci --rules @ci.ncl
error: rule 0 (job:write) must say which namespaces it covers
hint: add namespaces = ["default"] for one namespace, or ["*"] for every
      namespace, including ones created later

$ legion acl policy apply ops --rules @ops.ncl
error: rule 2 (node:read) has a `namespaces` selector, but node is cluster-scoped,
       so the rule could never match
```

The engine's one sentence, "a rule without a selector matches any value",
stays true. Validation just won't let a namespaced rule leave this one out.
"Every namespace, including future ones" is the reach that K8s ClusterRoleBindings
and Nomad `namespace "*"` blocks grant, and here too it is always something
an admin *wrote* (`["*"]`), never something they forgot. The second check
kills a whole class of silent no-op rules: `namespaces` on `kv` today, and
`namespaces` on `net`, which is the footgun in § 1.

**Exact names that don't exist yet produce a warning, not an error,**
because policies are often written before the namespaces they name:

```console
warning: rule 0 names namespace 'payments-prdo', which does not exist
```

Globs never warn: `payments-*` legitimately matches namespaces nobody has
created yet. The docs tell admins to include the delimiter (`payments-*`,
not `payments*`), because a prefix without one also matches `paymentsx`
(the Kafka lesson).

**Deny already works.** "Everything except the platform's namespaces" can
be written, which Kubernetes can't express (K7):

```nickel
[
  { resources = ["job"], actions = ["read"], namespaces = ["*"] },
  { resources = ["job"], actions = ["read"], namespaces = ["legion-*", "platform-*"],
    effect = "deny" },
]
```

**One new resource, `namespace`,** scoped by `namespaces`:

| Action | Grants |
| --- | --- |
| `read` | get a namespace (list is per-item, below) |
| `write` | create, update and delete namespaces whose names match. A cascade delete additionally needs `job:write` there (§ 8.11). |

This makes delegated creation a one-line rule:
`{ resources = ["namespace"], actions = ["write"], namespaces = ["payments-*"] }`
(N10). It is its own resource rather than part of `operator` for the
reason `scale` and `boot` are: "may create a namespace" must not imply "may
reconfigure raft" (call 12).

**Seeing namespaces** (K5). `ListNamespaces` returns a namespace when the
caller holds *any* allow in it: `namespace:read`, or a read or write on any
namespaced resource there. This is the "any capability" rule Nomad uses,
and the filtered list Kubernetes never shipped. `WhoAmIReply` gains
`readable_namespaces`, so the CLI and UI can choose defaults and hints
without probing.

**Enforcement mechanics.** `AclScope::job(name)` becomes
`AclScope::job(namespace, name)`. The same goes for `exec`, `action` and
`job_via_template`. That is a signature change, so all 54 construction
sites in `src/manager` become compile errors until each supplies a
namespace from a stored column. The coarse-then-precise pattern is
unchanged. `list_jobs`'s per-item `retain` filters by namespace with no
code beyond the new argument. The audit log's `namespace` field
(`src/manager/acl.rs:780`) gets a real value.

**Things that go live for free.** The `bands` selector composes with
`namespaces`, so "only payments may claim `high`" becomes real in Phase 1.
TODO.md's "per-namespace band caps" claim turns true here.

### 8.6 What a namespace does and does not isolate, plane by plane

This table goes into the public page near the top. It is the answer to K1,
written so no one has to infer it.

| Plane | A namespace… | It does **not**… | Real isolation comes from |
| --- | --- | --- | --- |
| Names | separates every job-keyed name (§ 8.2) | — | — |
| API and ACLs | scopes every namespaced RPC and list | hide cluster-scoped views: `node:read` sees every allocation's job id in the ledger (`proto :9234`); the state explorer is raw SQL over everything; `operator:read` sees all events | not granting `node`/`state`/`operator` to tenants |
| Service discovery and mesh | resolves names within the namespace; crossing is explicit (§ 8.7) | stop a process from dialing an IP it already knows | `network.mode = "gated"` plus net policies selecting namespaces |
| Network | nothing by itself | filter packets | `gated` plus net policies |
| Compute | nothing (until Phase 4 *restricts* which pools a namespace may use) | separate nodes, kernels or caches | node pools; the `sandbox` runtime |
| Images and artifacts | nothing today | stop a node serving a cached private image to a job without credentials for it (§ 6.2) | the fix tracked in TODO.md |
| KV | nothing | KV is path-scoped | `paths` selectors |
| Workload identity | makes subjects distinct, and adds a namespace claim (§ 8.8) | — | IdP trust policies that match the namespace |
| Capacity | nothing until Phase 4 | — | namespace policies (§ 8.12) |
| Observability | stamps events and metrics; tenants read their own events (Phase 2) | — | — |

### 8.7 Services and the mesh

This is Phase 1, not a later phase. Today's catalog pools same-named
endpoints across jobs (§ 1). The moment two namespaces can each advertise
`web`, a namespace that doesn't own its services sends team A's traffic to
team B's instances. That is a correctness and a confidentiality bug, and it
is exactly the Consul CE failure in N14.

**Phase 1, same-namespace only:**

- A service inherits its job's namespace. The catalog's identity is
  `(namespace, service)`: `service_endpoints`, `declared_services`, the
  per-heartbeat endpoint feed, and the in-memory edge window keyed
  `(job, task, service, tag)` all carry it.
- `connect` upstreams resolve in the consumer's own namespace. The short DNS
  form `<service>.svc.legion` keeps resolving there. Every existing jobspec
  stays correct, and the common case is the one you don't type.
- `allow` intentions accept a bare job name (same namespace) or a qualified
  id. A bare name never reaches outside its namespace.
- `ListServices`, `ResolveService` and `ListServiceEdges` gain per-item
  filtering by `job:read` in the service's namespace. Today they filter
  nothing (§ 1).
- Net-policy selector facts carry the real namespace (the 7 builders in
  § 6.1). So a net policy whose selector names `namespaces = ["payments-*"]`
  starts scoping on the day this ships, with no change to the policy surface.

**Phase 2, crossing on purpose.** Both ends must opt in. This is Gateway
API's ReferenceGrant lesson (K10), and it is the ask behind Nomad's most-upvoted
namespace issue (N15):

```nickel
# in checkout-prod: the consumer names the namespace it is reaching into
connect = { upstreams = [
  { service = "redis" },                                # own namespace
  { service = "auth", namespace = "shared" },           # explicit crossing
] },

# in shared: the target admits the consumer by qualified id or glob
service = { port = 8443, allow = ["gateway", "checkout-prod/api", "payments-*/api"] },
```

The fully-qualified DNS form becomes `<service>.<namespace>.svc.legion`.
That needs the node resolver to accept two labels under the zone
(`dns.rs:406`). The short form keeps meaning "my namespace", so the
two-label form is the crossing made visible in the config that does it. A
namespace name is a DNS label, and Legion answers only under `svc.legion`,
so a namespace called `com` shadows nothing (K18).

`allow` is not cryptographic identity, and it stays documented that way.
It controls what the catalog *offers* a consumer; enforcement on the wire is
`gated` plus net policies. Tenant-authored net policies, where a team writes
policy for its own namespace, are deferred. With delegated authorship, the
`net:write` check would have to prove that a selector's namespace globs are
covered by the caller's globs. That is glob subsumption, a problem worth
its own design.

### 8.8 Workload identity

**The second-sharpest security item in the plan.** Once two namespaces can
each hold `web`, today's default subject `{region}:{job}:{group}:{task}`
mints **identical subjects for different tenants**, and an IdP trust policy
written for one would accept the other's tokens.

- `{namespace}` becomes a stable placeholder (`STABLE_SUB_FIELDS`,
  `workload_identity.rs:82`).
- The default becomes `{region}:{namespace}:{job}:{group}:{task}`, which
  renders as `global:payments-prod:web:frontend:nginx`. That is Nomad's
  composed subject; the "minus the namespace segment" comment goes away.
- **The uniqueness floor gains `{namespace}`** (`:543`). A cluster
  `subTemplate`, an `allowedSubTemplates` entry, or a per-identity
  `subTemplate` that drops it is rejected at config load or submit, with the
  same message shape as a dropped `{job}`. There is no "pin the old
  template" escape hatch (call 8). The floor already hard-rejects a dropped
  `{job}`, and dropping `{namespace}` is the same forgery.
- Tokens carry a `legion.io/namespace` claim. Control-plane binding rules
  (`AclBindingRule`, `src/structs/config.rs:1503`) can match on it, so a
  platform can give every workload in `payments-*` one policy without
  attaching it job by job (N11).

### 8.9 Events, metrics and audit

- **Events carry `namespace`** from Phase 1. Cluster-scoped events (nodes,
  raft, the scheduler) carry none.
- **Tenants read their own events from Phase 2.** `list_events` and
  `stream_events` keep the `operator:read` path, which sees everything. They
  gain a second path: a caller with `job:read` in some namespaces receives
  those namespaces' events, filtered per item like every other list. **`*`
  only widens the candidate set and never skips the filter** (N6: Nomad's
  event-stream wildcard CVE was a separate validation path for `*`).
  `stream_events` re-authenticates on its interval as it does today, so a
  revoked grant stops the stream.
- **Metrics.** Legion's families are low-cardinality, and each caps at 256
  label combinations with overflow folded into `"other"`
  (`docs/metrics.md:70-72`). Phase 2 adds a `namespace` label to every
  family about workloads: submits, placements, preemptions, allocation
  states, and the mesh request and byte families. It also adds
  `legion_namespace_reserved_{cpu,memory}` gauges, which become the quota
  usage view in Phase 4. Every workload family gets the label, not some of
  them (N18), and the fold at 256 namespaces is documented.
- **Audit** records the real namespace (§ 8.5).

### 8.10 The rest of the job-keyed and cluster-wide state

| Object | Today | With namespaces | Phase |
| --- | --- | --- | --- |
| Job versions, rollouts, scale, snapshot aliases, boot/exec/session tickets, KV references, volume claims, seal and chunk keys, content-store manifests | keyed by the bare job id | inherit the namespace through the qualified id (§ 8.2) | 1 |
| Keyed instances | `instance_id` unique cluster-wide | unique per namespace | 1 |
| Idempotency records | per caller and operation, fingerprinted | unchanged; the namespace is part of the fingerprinted request, so reusing a key across namespaces is `InvalidInput` | — |
| Job templates | cluster objects | **stay cluster objects** (call 20). `RunJobTemplate` takes a target namespace, and the collision guard becomes namespace-local, which confines today's oracle to namespaces the caller can submit into | 1 |
| Standalone volumes (no owning job) | cluster-wide; deleting needs cluster-wide `job:write` (`docs/volumes.md:373`) | carry a namespace | 2 |
| Exec keys | `jobs` glob over bare names, which crosses tenants (`src/manager/exec.rs:424`) | patterns match qualified ids (`payments-*/web`) | 2 |
| Host volumes | any `job:write` holder can claim any name declared on a node | an allowlist per namespace policy (`hostVolumes`) | 4 |
| Net policies | cluster objects; `namespaces` selector matched against `default` | cluster objects; selector facts are real | 1 |
| KV, saved queries, the state explorer, the ledger | cluster-scoped | **stay** cluster-scoped and documented as such: operator tools | — |

### 8.11 Deletion

```console
$ legion namespace delete payments-old
error: namespace 'payments-old' is not empty: 3 jobs, 1 volume, 2 snapshots
hint: `legion namespace delete payments-old --dry-run` lists them; --cascade removes them

$ legion namespace delete payments-old --cascade
Stopping payments-old/web ... stopped (graceful, 30s kill timeout honoured)
Stopping payments-old/worker ... stopped
Destroying 2 jobs ... done
Deleting volume payments-old/data ... done
Deleting 2 snapshots ... done
Deleted namespace payments-old.
```

- **What counts as "not empty"** is one list in one place: jobs in any state,
  standalone volumes, snapshots, and instances. The refusal counts each kind
  (K3). There is no per-kind patchwork that each new object type has to
  remember to join (N12). A test asserts that every table carrying a
  namespace column is in the list.
- **The final delete is atomic and conditional.** "Delete if empty" is one
  store write. A submit that races in between the cascade's sweep and that
  write makes the delete refuse, saying what arrived. Re-running finishes
  the job. Nothing ever succeeds falsely (N12, #19414).
- **No `Terminating` state, and nothing to depend on.** The cascade uses the
  ordinary stop and destroy paths, which already handle lost nodes. It
  never waits on an add-on, a plugin, or a reachable worker (K2: the
  Datadog incident). If it fails halfway, the namespace is still there,
  still deletable, and `--dry-run` shows what remains.
- **Fixed order:** stop jobs (graceful), destroy job rows, delete volumes
  and snapshots, delete the namespace. Net policies are not namespace
  contents, so nothing that protects a running task is removed before the
  task stops (K4).
- **A cascade needs `job:write` in the namespace as well as
  `namespace:write`.** Without that, the right to delete an environment would
  be a back door to destroying workloads the deleter couldn't otherwise
  touch (K9).
- **`DeleteNamespace` takes `dry_run` and `idempotency_key`** (AGENTS.md),
  so an agent can preview the cascade and safely retry it.
- **Name reuse is allowed, and visible.** A deleted namespace's name can be
  created again. Policies select by name, so a recreated `payments-old`
  inherits every grant that names it. The workload subjects of any jobs
  resubmitted with the same names also match whatever IdP trust was written
  for the old ones. That is the GCP "IDs are never reused" lesson, and we
  answer it with visibility rather than tombstones:

  ```console
  $ legion namespace create payments-old --dry-run
  Would create namespace 'payments-old'.
  These policies would reach it:
    platform-admin   job:*        namespaces = ["*"]
    payments-team    job:*        namespaces = ["payments-*"]
  ```

### 8.12 Governance: namespace policies (Phase 4)

The first version of this plan put quota, allowed pools and allowed runtimes
**on the namespace object**. The research broke that design twice:

1. **Delegated creation dodges per-namespace quota.** If the payments lead
   may create `payments-*` namespaces, a quota on `payments-prod` is
   escaped by creating `payments-prod2`. That is why every Kubernetes tenancy
   product (OpenShift ClusterResourceQuota, Rancher project limits, Kiosk
   AccountQuota, HNC HierarchicalResourceQuota) added a quota that spans
   namespaces (K12, K13).
2. **The namespace writer would be the quota writer.** Whoever holds
   `namespace:write` on a namespace could raise its own limits. That is
   Kubernetes' namespace-label escalation (K9) in a new place.

So governance lives in **namespace policies**: cluster objects that select
namespaces by glob, the same shape net policies already have.

```nickel
# legion namespace policy apply payments -f payments.ncl
{
  name = "payments",
  namespaces = ["payments", "payments-*"],
  # Aggregate over EVERY matching namespace, per priority band (Borg).
  # Bands not listed are uncapped; `background` is uncapped unless named.
  quota = {
    high   = { cpu = 64000,  memory = "128Gi" },
    normal = { cpu = 200000, memory = "512Gi" },
  },
  maxJobs = 300,
  maxNamespaces = 8,                                   # caps delegated creation
  pools = { allowed = ["shared-*", "payments-gpu"], default = "shared-general" },
  runtimes = ["container", "sandbox"],                 # Nomad's driver allowlist
  hostVolumes = ["payments-*"],
}
```

- **Aggregate, so delegation can't dodge it.** Scenario 3: `payments-loadtest`
  lands inside payments' totals the moment it is created.
- **Several policies can match one namespace, and all of them apply.** Each
  quota is checked independently (CF's org plus space). Allowlists
  intersect, so a second policy can only narrow (Pulsar, AWS SCPs). This
  means there is no precedence rule to learn.
- **Checked at admission, not in placement** (Borg: "quota-checking is part
  of admission control, not scheduling"). A submit over quota, outside its
  allowed pools, or on a disallowed runtime fails at submit, where the user
  can fix it:

  ```console
  error: namespace policy 'payments' caps band 'high' at 64000 MHz CPU across
         payments, payments-*; in use 61500, this job adds 4000
  hint: `legion namespace get payments-prod` shows usage; lower the band or the count
  ```

  It is re-checked at plan apply, so two racing submits can't both squeeze
  under the cap. That is the same reason preemption validates at apply.
- **Quota never forces a jobspec change.** Every task already has a
  reservation: its own, or `taskResourceDefaults`. So Kubernetes' "quota
  requires requests, so add a LimitRange" problem has no Legion equivalent
  (K12). Turning quota on can reject new submits, but it never evicts
  running work and never breaks a job that was already admitted.
- **Gated on `acl:write`** for writes and `acl:read` for reads. Whoever may
  write ACL policy could grant anything anyway, so putting governance at the
  same privilege adds no new escalation path. A namespace writer can't touch
  it. Tenants still see their *effective* limits and usage through
  `legion namespace get`, which needs only `namespace:read`.
- **Bands stay in ACL rules** (call 9). The `bands` selector answers *who*
  may claim `high`. The quota answers *how much* `high` a group of
  namespaces may hold. These are different questions, and the preemption
  work already rejected a second band vocabulary.

---

## 9. Non-namespace ways to reach the same goals

The goals are: (a) permission scoping, (b) collision-free names,
(c) legibility of a shared cluster, (d) a governance anchor, (e)
blast-radius containment.

| Mechanism | Status | Delivers | Doesn't deliver |
| --- | --- | --- | --- |
| Name prefixes plus `names` globs | shipped | (a) mostly; (b) by convention | (c), (d), (e) |
| Node pools plus the `pools` selector | shipped | compute isolation | naming; job permissions |
| Job templates as the submit channel | shipped | (a) and (e) strongly | (b), (c), (d) |
| Priority bands plus the `bands` selector | shipped | contention governance | (a), (b), (c) |
| KV path prefixes | shipped | (a) for KV | everything else |
| Separate clusters plus federation | planned | everything, strongly | cheapness (B1) |
| Labels plus label-selector ACLs | not built | (a), N-dimensionally | (b), and it is unsafe (§ 7 B) |
| Per-token quota (Borg's model) | not built | (d) | (a), (b), (c); and three CI tokens would mean three quotas |

**Job templates are already a stronger guardrail than namespaces.** A team
restricted to `templates = ["render"]` can't submit an arbitrary spec at all.
If the requirement is "this team may only run these workloads", templates
are the right tool today. Namespaces complement them: a template run lands
in a namespace.

**What only namespaces deliver:** collision-free names; one anchor for
tenant-wide facts; legibility; a rule that keeps working as Legion grows;
and the word every operator arriving from Kubernetes or Nomad already
knows. If those aren't worth § 12's cost, the alternative is to document
the convention, improve `names`-glob ergonomics, and stop. This section is
written so that decision stays available.

---

## 10. The UI

The UI is where a tenancy model is either legible or useless. It is also
where Nomad's namespace bugs piled up (N7, N8).

**Shapes considered:**

1. A sidebar switcher with the selection in localStorage, as in the K8s
   Dashboard and the Nomad UI. Links aren't shareable: a pasted job URL shows
   the recipient their own namespace, mid-incident.
2. The namespace in the URL path (`/jobs/payments-prod/web`).
3. A query-parameter filter only. That works for lists and fails for detail
   pages once names repeat.
4. A namespace column and filter only, with no switcher.
5. A full workspace switch that reloads the app, as in Grafana orgs. Too
   heavy, and the operator loses the cross-tenant view.

**Chosen: 2 + 1, with 4 as the All view.**

- **Detail routes carry the namespace as a path segment:**
  `/jobs/:namespace/:name/...`, `/services/:namespace/:name`, and
  `/volumes/:namespace/:id`. Navigation can't drop a path segment the way
  Nomad's UI kept dropping `?namespace=` (N7: #12022, and the exec window
  fixed three times). Routes keyed by a global numeric id (`/rollouts/:id`,
  allocations) stay as they are, and show the namespace in their header.
  **There are no redirects from old `/jobs/:name` URLs.** There is no
  installed base of bookmarks (AGENTS.md).
- **List pages carry the selection as a query parameter**
  (`/jobs?ns=payments-prod`, `ns=*`). A filter is expressed as a filter.
- **The switcher sits in the page header, not the sidebar.** The sidebar
  navigates between *kinds* of thing, and the namespace is a property of what
  you're looking at. On a detail page the switcher is shown but disabled:
  choosing a namespace navigates to that namespace's list, rather than
  stranding you on a page that no longer exists.
- **On cluster-scoped pages** (Nodes, Manager, Raft, Observability, State)
  there is **no** switcher, and the header has a "cluster-scoped" tag. The
  Network page keeps the switcher as a filter over which policies apply, and
  says so.
- **"All namespaces" is a real selection,** and it paginates (N7: #11970).
  It is the default for an identity that can read more than one namespace.
- **Single-namespace identities never see tenancy UI** (call 23). If
  `readable_namespaces` has one entry, there is no switcher, just a static
  badge.
- **Every detail header shows `payments-prod / web`,** with the namespace
  linking to its list.
- **A Namespaces page** (in a new Governance section) lists namespaces with
  job counts, the policies that reach each one (the blast-radius view from
  § 8.11 rendered), and, from Phase 4, the matching namespace policies and
  usage.

**The client-side ACL mirror** (`ui/src/utils/acl.ts`) gains the namespace
in `jobScope` and a `readableNamespaces()` helper. It also catches up on its
drift (`boot`, `bands`, `host` on `job`; § 6.1) in Phase 1, because it is
about to compute which tenancy UI to show. Drift there now hides the wrong
controls.

Phase 1 ships only what identity forces: path-segment routes, the
`?ns=` list filter, a `NAMESPACE` column under `ns=*`, and the header badge.
Phase 3 ships the switcher, the All view's grouping, single-namespace
suppression, and the Namespaces page.

```
┌────────────────────────────────────────────────────────────────────┐
│  Jobs ▸ payments-prod ▸ web                     [payments-prod ▾]  │
│  web   payments-prod   running   6/6 healthy   high   3m ago       │
└────────────────────────────────────────────────────────────────────┘
```

---

## 11. Proto, struct, config and persistence changes

### Proto (`proto/legion.proto`)

Every message and field gets the full public-facing comment the repo
requires. The comment on `Namespace` states the isolation non-goal in its
first paragraph, because the proto is the API's source of truth and that is
the misunderstanding this feature will generate.

```proto
message Namespace {
  string name = 1;
  string description = 2;
  map<string, string> meta = 3;
  int64 created_at = 4;
  int64 updated_at = 5;
}

rpc ListNamespaces(ListNamespacesRequest) returns (ListNamespacesReply);    // per-item filtered
rpc GetNamespace(GetNamespaceRequest) returns (GetNamespaceReply);          // + counts by kind
rpc ApplyNamespace(ApplyNamespaceRequest) returns (ApplyNamespaceReply);    // create or update; dry_run
rpc DeleteNamespace(DeleteNamespaceRequest) returns (DeleteNamespaceReply); // cascade, dry_run, idempotency_key
// Phase 4: NamespacePolicy + List/Get/Apply/DeleteNamespacePolicy.
```

- **Addressing a job:** every request that names a job gains
  `string namespace` (empty means `default`). Its existing name field
  accepts either `web` or `payments-prod/web`, through one shared parser in
  `src/structs/namespace.rs`. A qualified name that disagrees with
  `namespace` is `InvalidArgument`. The parser is shared because Legion's
  goal is that you can paste back what you were shown.
- **Lists:** `repeated string namespaces` (names or globs; empty means
  `default`; `["*"]` means all) on `ListJobs`, `ListInstances`,
  `ListRollouts`, `ListServices`, `ListVolumes`, `ListSnapshots`,
  `ListEvents` and `StreamEvents`.
- **Replies:** `Job`, `JobConfig`, `Instance`, `Allocation`, `Rollout`,
  `Service`, `Volume`, `Snapshot` and `Event` gain `string namespace` and,
  where they carry a `job_id`, `string job_name`. **Every `job_id` on the
  wire is the qualified id.**
- `WhoAmIReply` gains `repeated string readable_namespaces`.

### Structs

- `src/structs/namespace.rs` (new) holds:
  - `Namespace` and `validate_namespace_name`.
  - The single `DEFAULT_NAMESPACE`. The copies in `acl.rs` and
    `netpolicy.rs` are removed, not re-exported.
  - `JobRef { namespace, name }` with `parse_job_ref` and `qualified_id`,
    used by the CLI, MCP and server alike.
- `src/structs/job.rs`: `namespace: Option<String>` on the jobspec.
- `src/structs/acl.rs`:
  - `AclResource::Namespace`.
  - `AclResource::is_namespaced()`.
  - The scope constructors take a namespace.
  - `AclScope::namespace(name)`.
  - The two `parse_rules` checks (§ 8.5).
- `src/structs/netpolicy.rs`: the facts take a real namespace.
- `src/structs/workload_identity.rs`: the placeholder, the floor, the new
  default, and the claim.

### Persistence (`src/storage/migrations.rs`)

```sql
CREATE TABLE IF NOT EXISTS namespace (
  name        STRING PRIMARY KEY,
  description STRING NOT NULL DEFAULT '',
  meta        JSONB  NOT NULL DEFAULT '{}',
  created_at  TIMESTAMP NOT NULL,
  updated_at  TIMESTAMP NOT NULL
);
INSERT OR IGNORE INTO namespace (name, description, created_at, updated_at)
  VALUES ('default', 'Default namespace', ...);

ALTER TABLE job ADD COLUMN namespace STRING NOT NULL DEFAULT 'default';
CREATE INDEX idx_job_namespace ON job(namespace);
-- job.id is now "{namespace}/{name}"; the PK is unchanged and still enforces uniqueness.
```

- **The `namespace` table replicates on the control lane** like every other
  control-plane table. There is no new lane and nothing multi-raft-specific.
- **No data migration** (call 18). The migration refuses to run on a store
  that already holds jobs, and says what to do:

  ```text
  error: this data directory holds jobs from before namespaces (schema 1xx).
  Job ids are now "namespace/name" and are not rewritten in place.
  Back up your jobspecs, start the manager on an empty data dir, and resubmit.
  ```

- `store.get_job(name)` becomes `get_job(&JobRef)`. The old signature is
  removed, so the compiler finds every caller.

### Config

- **Manager config:** nothing new. `identity.subTemplate`'s default
  changes (§ 8.8).
- **Phase 4** adds nothing to config either. Namespace policies are store
  objects, which is the point of having objects.

### Agent surfaces

- `catalog()` in `src/cmd/capabilities.rs` gains `namespace list|get|apply|delete`.
  `apply` and `delete` are `mutates: true, read_only_safe: false`. Phase 4
  adds the `namespace policy` commands.
- MCP (`src/cmd/mcp.rs`):
  - `legion_namespace_list` and `legion_namespace_get` are read-only, so they
    are always listed.
  - `legion_namespace_apply` and `legion_namespace_delete` are
    `mutates: true`, gated behind `--allow-write`.
  - Every job tool gains `namespace` and returns qualified ids.
- `docs/agent-readiness.md`: a namespace section, recommending
  one-namespace tokens for agents (B3).

---

## 12. Implementation phases

Each phase ships alone and is worth shipping alone. Phase 1 is large because
the minimum honest namespace is large. Shipping identity without services
would route traffic across tenants (§ 8.7). It lands as an ordered stack of
PRs (1a → 1d), and the public page says at each step what is live.

### Phase 1 — Namespaces are real

- **1a. The object.** The table, struct, validation and four RPCs. The
  `legion namespace` CLI, MCP tools and catalog entries.
  `AclResource::Namespace`, the per-item namespace list, and
  `readable_namespaces`. The two `parse_rules` checks. Endpoint-table
  entries.
- **1b. Identity.** The jobspec field and server-side resolution. Qualified
  ids. The new `AclScope` signatures at all 54 sites. `-n`,
  `LEGION_NAMESPACE`, `-A`, qualified references, and the not-found,
  empty-list and denied hints. Per-namespace keyed instances.
  `RunJobTemplate` taking a target namespace. `$LEGION_NAMESPACE` in tasks.
  The `{namespace}` subject floor, default and claim. The namespace field
  on events. Delete with refuse, `--dry-run` and `--cascade`. The refusal
  migration.
- **1c. Services.** The catalog keyed `(namespace, service)`.
  Same-namespace `connect` and DNS. Qualified `allow`. Per-item filtering on
  the three service RPCs. Real net-policy facts.
- **1d. Minimum UI.** Path-segment routes, the `?ns=` filter, the namespace
  column and badge, and the ACL mirror caught up.

*Value on its own:* two teams share a cluster, each with a job called
`web`, with scoped tokens, distinct workload identities, and no traffic or
service visibility crossing between them.

### Phase 2 — Crossing on purpose, and seeing your own

- Cross-namespace `connect` upstreams and target-side `allow` by qualified
  id or glob.
- The `<service>.<namespace>.svc.legion` DNS form and the resolver change.
- Tenant-readable events with per-item filtering.
- `namespace` labels on workload metrics, and the reserved-resource gauges.
- Namespace lifecycle events.
- Exec-key patterns over qualified ids.
- Namespaced standalone volumes.

*Value on its own:* shared platform services (`shared/auth`) are reachable
deliberately, and a tenant can watch its own work without `operator:read`.

### Phase 3 — The UI

The header switcher, All-namespaces grouping and pagination, single-namespace
suppression, the Namespaces page and blast-radius view, and detail-page
switcher behavior.

*Value on its own:* the shared cluster becomes legible.

### Phase 4 — Governance

Namespace policies:

- Aggregate per-band quota, `maxJobs` and `maxNamespaces`.
- Allowed and default pools.
- Allowed runtimes and host volumes.
- Admission checks, with a re-check at plan apply.
- Usage in `legion namespace get` and on the Namespaces page.
- A `--dry-run` that shows which existing jobs a new policy would have
  rejected.

*Value on its own:* one team's bad afternoon stops being everyone's.

---

## 13. Testing plan

**Cross-cutting suites, added in Phase 1 and extended in each phase:**

- **The namespace-isolation matrix** (`tests/acl_endpoints.rs`). Every
  Manager RPC whose scope is namespaced is called with a `payments-*` token
  against a `checkout-prod` object, and must return `PermissionDenied`, not
  `NotFound` or success. The existing `every_manager_rpc_is_classified` test
  gains a second assertion: every RPC is classified as namespaced or
  cluster-scoped. This is the same "can't forget one" guarantee, extended to
  the new axis.
- **Wildcard parity** (N4, N6). A test parses the proto. Every
  `List*`/`Stream*` RPC over a namespaced kind must carry `namespaces`. Each
  one is called with `["*"]` and a one-namespace token, and must return only
  that namespace's items.
- **Same name, two namespaces.** `payments-prod/web` and `checkout-prod/web`
  run concurrently through submit, rollout, scale, exec, snapshot, checkpoint
  and destroy, with no interference. Their seal keys differ, their identity
  subjects differ, and their content-store paths differ.
- **Longest names** (K18). A 63-character namespace with a 128-character job
  and task go through every path that embeds the qualified id:
  content-store manifests, session manifests, allocation directories, log
  paths, and DNS answers. Failures are fixed by hashing a component, never
  by silently truncating it.
- **Service isolation.** Same-named services in two namespaces resolve to
  their own endpoints. `ListServices` returns only permitted items, and the
  edge window doesn't mix them.
- **Spec/request conflict** (N5). Every spec-bearing RPC rejects a
  disagreeing namespace, and authorizes against the resolved one, never the
  request's.
- **Regression.** Every existing test is updated once, for the qualified-id
  wire shape and the explicit-`namespaces` policy rule, and then passes. A
  test that needs more than that is a design bug to raise, not a fixture to
  patch.

**Per phase:**

- **Phase 1:**
  - Unit: name grammar and reserved names; `default` undeletable;
    `parse_job_ref`; both `parse_rules` checks; the subject floor.
  - Integration: the full lifecycle in a namespace, plus refusal into a
    missing namespace.
  - Cascade: ordering; a racing submit makes the final delete refuse;
    re-running finishes; a cascade without `job:write` is refused.
  - `allow` with a bare name never matches across namespaces.
- **Phase 2:**
  - Cross-namespace upstreams fail without the target's `allow` and succeed
    with it.
  - The two-label DNS form resolves.
  - A tenant token streams only its own events, and revoking the grant
    stops the stream.
  - Metric labels are present on every workload family (a test walks the
    registry).
- **Phase 3:** a one-namespace identity is a first-class UI fixture (N8).
  - Every page renders and every action button's enabled state matches the
    ACL mirror.
  - The switcher is absent.
  - A pasted detail URL opens the same object for a second user.
  - Mirror parity cases are shared with the Rust tests.
- **Phase 4:**
  - Quota arithmetic per band and in aggregate across matched namespaces.
  - A delegated `payments-new` lands inside payments' totals.
  - Two racing submits can't both pass at apply.
  - Allowlist intersection across two policies.
  - Rejection at submit, not at placement.

**Benchmarks.** Phases 1–3 don't touch the scheduler's placement inputs.
The qualified id lengthens string keys a little, so Phase 1 runs
`cargo bench --bench scheduler_bench -- candidate_construction` before and
after, to show the key change is noise. Phase 4's admission check sits on
the submit path, with a re-check at plan apply. It ships with before and
after numbers from the relevant `scheduler_bench` groups, per AGENTS.md. Its
design keeps a counter per `(namespace, band)`, maintained on write, rather
than aggregating at admission, and avoids a single hot row (K12: #67761).

---

## 14. Rollout, and what breaks

Legion has no installed base (AGENTS.md), so this ships **default-on, with
no feature flag**. It is inert until someone creates a second namespace:
everything lives in `default`, and `default` is the active namespace.

**What breaks, for the PR descriptions:**

1. **Pre-namespace data directories are refused.** Wipe and resubmit
   (call 18).
2. **Every ACL rule over a namespaced resource must name its namespaces.**
   Existing policies, the docs examples and the test fixtures are all
   rewritten (call 3).
3. **Workload-identity subjects gain the namespace segment.** IdP trust
   policies that match on `sub` are rewritten, and a template without
   `{namespace}` is rejected (call 8).
4. **Wire shape.** `job_id` fields carry qualified ids. UI detail routes
   change, with no redirects.
5. **Instance ids are unique per namespace,** no longer cluster-wide.
6. **`net show` and `net test`** check `job:read` on the job instead of a
   job-scoped `net` rule.

**Docs that land with the phases, not after:**

- **A new `docs/namespaces.md`.** Its first section is the § 8.6 table. It
  also covers the surface, the defaults, the failure modes (not-found in the
  wrong namespace, a missing namespace, non-empty delete, the explicit
  `namespaces` rule, name reuse) and the known limitations, starting with
  the image-cache gap until that is fixed.
- **`docs/auth-and-acls.md`:** the selector becomes real, plus the new
  resource, the two validation rules, and the delimiter advice. The note
  that "Legion has no user-defined namespaces" (`:186-191`) is removed.
- **`docs/network-handlers.md`:** remove the "no job namespaces" notes
  (`:263`, `:1728`), and fix `configs/policies/sandbox-egress.ncl:21`.
- **`docs/service-discovery.md`:** catalog scoping, DNS forms, crossing.
- **`docs/workload-identity.md`:** the new default subject, the floor, the
  claim.
- **`docs/volumes.md:373`, `docs/preemption-and-eviction.md:58`,
  `docs/suspend-and-resume.md:2021`, `docs/session-mode.md:210`:** these
  describe namespaces as already existing and become accurate.
- **`docs/agent-readiness.md`, `docs/upgrades.md`, `docs/README.md`, and
  the doc list in `README.md`.**
- **`configs/jobs/maximal.ncl`:** the `namespace` field.

**TODO.md** gets the phases as open items now, and check-offs per phase. The
pre-existing problems this recon found get their own items: the image-cache
credential bypass, the template-guard existence oracle, and the UI
ACL-mirror drift. The `action_names` spelling in
`docs/auth-and-acls.md:118` was fixed in the same change as this revision.

---

## 15. Open questions

1. **How is quota usage counted?** One counter row per `(namespace, band)`
   is accurate and contended. Per-node partial sums are cheap and eventually
   consistent. *Lean:* counters on the status lane, updated by the same
   write that transitions an allocation's reservation, with the authoritative
   check at plan apply. Resolve with measurements in Phase 4.
2. **Should the UI default to All for multi-namespace identities?** The CLI
   won't: Nomad declined a `*` default because of accidental actions (N3).
   *Lean:* the UI yes, the CLI no. The asymmetry is defensible because the
   UI's destructive actions sit on detail pages, which are always a single
   namespace.
3. **Should namespace policies be gated by `acl:write` or by their own
   resource?** *Lean:* `acl:write`. Splitting them out only helps if
   someone should set quotas without being able to write ACLs, and no one has
   asked for that.
4. **Should `default` be optional?** Kubernetes advises against using it in
   production (K14). *Lean:* no. A built-in `default` is what keeps
   single-team clusters free of the concept, and Temporal shows the friction
   of making users create it.
5. **Namespace sameness under federation.** SIG-Multicluster treats a name
   as meaning the same tenant in every cluster
   ([namespace sameness](https://multicluster.sigs.k8s.io/concepts/namespace-sameness)).
   *Lean:* adopt it. The federation plan owns replication, and Nomad's
   replication bugs are its test list: `namespace apply -region=other`
   silently wrote locally ([#20128](https://github.com/hashicorp/nomad/issues/20128)),
   and followers without a replication token tried to delete every namespace
   ([#17536](https://github.com/hashicorp/nomad/issues/17536)).
6. **Cross-namespace job dependencies.** A future dependency feature should
   assume qualified ids. This is flagged so it doesn't assume a flat world.
7. **Templated self-service creation** (OpenShift ProjectRequest). *Lean:*
   CLI scaffolding (`legion namespace create --with-policy payments`) that
   writes an ACL policy file for review. No server-side template concept
   until someone asks twice.

---

## 16. Acceptance criteria

**Phase 1.**

- `legion namespace create|list|get|apply|delete` work over CLI, gRPC and
  MCP with `--json` envelopes. `default` can't be deleted.
- A write into a missing namespace is refused, naming it.
- A rule over `job` without `namespaces` is rejected at apply, and so is
  `namespaces` on `node`.
- A `payments-*` token runs the full job lifecycle in `payments-prod`, and
  receives `PermissionDenied` on every equivalent operation in
  `checkout-prod`, including with `-A`.
- `payments-prod/web` and `checkout-prod/web` run concurrently. Each one's
  `connect` upstream `web` reaches its own instances. Their identity
  subjects, seal keys and content-store paths differ.
- `ListServices` with a `payments-*` token shows only payments services.
- A cascade with a racing submit refuses at the final delete, and a re-run
  completes it.
- The namespace-isolation matrix and the wildcard-parity test pass.

**Phase 2.**

- A cross-namespace upstream fails without the target's `allow` and
  succeeds with it.
- `auth.shared.svc.legion` resolves for an allowed consumer.
- A tenant token streams only its namespace's events, and stops receiving
  them when its grant is revoked.
- Every workload metric family carries `namespace`.

**Phase 3.**

- A detail URL pasted to another operator opens the same object.
- A one-namespace identity sees no switcher anywhere, and every enabled
  button works.
- The Namespaces page lists every readable namespace with job counts and
  the policies that reach it.

**Phase 4.**

- A submit that would take payments' aggregate `high` CPU over its cap is
  rejected at submit, with a message naming the policy, the usage and the
  increment.
- A newly delegated `payments-*` namespace counts against the same totals.
- A job constrained to a disallowed pool is rejected at submit, not left
  unplaced.
- Usage is visible in `legion namespace get -j` and on the Namespaces page.

---

## Appendix A. Sources by section

The tables in § 2 and § 3 cite inline. The primary documents most worth
reading in full:

- **Kubernetes:**
  [multi-tenancy concepts](https://kubernetes.io/docs/concepts/security/multi-tenancy/),
  [EKS tenant isolation](https://docs.aws.amazon.com/eks/latest/best-practices/tenant-isolation.html),
  [RBAC good practices](https://kubernetes.io/docs/concepts/security/rbac-good-practices/),
  [#60807 (Terminating)](https://github.com/kubernetes/kubernetes/issues/60807),
  [#48537 (list my namespaces)](https://github.com/kubernetes/kubernetes/issues/48537),
  [#44703 (label RBAC declined)](https://github.com/kubernetes/kubernetes/issues/44703),
  [#85963 (deny rules)](https://github.com/kubernetes/kubernetes/issues/85963),
  [#70147 (ClusterSecret)](https://github.com/kubernetes/kubernetes/issues/70147),
  [ReferenceGrant](https://gateway-api.sigs.k8s.io/reference/api-types/referencegrant/),
  [KEP-2161](https://www.kubernetes.dev/resources/keps/2161/),
  [HNC (retired)](https://github.com/kubernetes-retired/hierarchical-namespaces),
  [Datadog's KubeCon 2020 talk](https://raw.githack.com/sbueringer/kubecon-slides/master/slides/2020-kubecon-na/How%20the%20OOM-Killer%20Deleted%20My%20Namespace,%20and%20Other%20Kubernetes%20Tales%20-%20Laurent%20Bernaille,%20Datadog%20-How%20the%20oom-killer%20deleted%20my%20namespace.pdf).
- **Nomad:**
  [namespace spec](https://developer.hashicorp.com/nomad/docs/other-specifications/namespace),
  [ACL policy spec](https://developer.hashicorp.com/nomad/docs/other-specifications/acl-policy),
  [#18803 (multi-tenant meta issue)](https://github.com/hashicorp/nomad/issues/18803),
  [#14177 (cross-namespace services)](https://github.com/hashicorp/nomad/issues/14177),
  [HCSEC-2024-27](https://discuss.hashicorp.com/t/hcsec-2024-27-nomad-vulnerable-to-cross-namespace-volume-creation-abusing-csi-write-permission/71212),
  [HCSEC-2025-02](https://discuss.hashicorp.com/t/hcsec-2025-02-nomad-vulnerable-to-event-stream-namespace-acl-policy-bypass-through-wildcard-namespace/73191),
  the [changelog](https://github.com/hashicorp/nomad/blob/main/CHANGELOG.md)
  (search for "namespace").
- **Tenancy at scale:**
  [Borg (EuroSys 2015)](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/43438.pdf)
  §2.5 and §5,
  [Twine (OSDI 2020)](https://www.usenix.org/system/files/osdi20-tang.pdf)
  §7.1,
  [CNCF/FinOps 2021](https://cncf.io/wp-content/uploads/2021/06/FINOPS_Kubernetes_Report.pdf).

Reaction and star counts are as of 2026-10-18. Stack Overflow couldn't be
reached by the research tooling, so no vote counts from it appear here.
