# Auth & ACLs

Legion uses token-based ACLs to control who can do what in a cluster. Callers
present a bearer credential, and the manager resolves it to a set of named
**policies**. Each policy contains scoped allow/deny **rules** over a small
set of resources and actions. Once ACLs are enabled, anything not allowed is
denied, and a matching deny rule always overrides an allow.

**ACLs are on by default.** Every API call needs a credential. A fresh
manager mints its own first credential, so nothing is open while you set it
up, and there is no bootstrap endpoint for anything else to race you to.

## Quick start

```bash
# 1. Start a manager. At first start it mints the initial management token
#    and writes the secret, mode 0600, to <dataDir>/bootstrap-token and to
#    the CLI token file for the manager's address, ~/.legion/tokens/127.0.0.1_4747:
legion start manager --run-worker
#   INFO ACLs are on. Created the initial management token (accessor 7c1f...).
#        Secret written to .legion/bootstrap-token and
#        /home/me/.legion/tokens/127.0.0.1_4747 (mode 0600).
#        The CLI on this machine uses it automatically. Treat it like a root password.

# 2. The CLI on the same machine already works: it reads the token stored for
#    the address it dials (LEGION_ADDR, default http://127.0.0.1:4747).
legion job run configs/jobs/demo.ncl

# 3. Create a policy and a narrower token for everyone and everything else.
legion acl policy apply dev-team --rules '[
  {"resources": ["job"],  "actions": ["read", "write"], "namespaces": ["default"], "names": ["dev-*"]},
  {"resources": ["kv"],   "actions": ["read", "write"], "paths": ["dev/*"]},
  {"resources": ["node"], "actions": ["read"]}
]'
legion acl token create --name dev-ci --policy dev-team --ttl 720h

# 4. Check what any credential can do.
legion acl whoami
```

On another machine, export the secret (`LEGION_TOKEN=lgn_...`) or run
`legion login`. The UI's login page takes the same secret.

### Where the initial token goes

| File | Written | Mode |
| --- | --- | --- |
| `<dataDir>/bootstrap-token` (`$LEGION_DATA_DIR`, else `./.legion`) | every time the manager mints the initial token | `0600` |
| `~/.legion/tokens/<host>_<port>` for the user running the manager, keyed by the address a CLI on that machine dials (`$LEGION_TOKEN_FILE` overrides the path) | every time the manager mints the initial token | `0600` |

The manager logs the accessor ID and the file paths once, and never logs the
secret.

The CLI file is named for the manager's local address: its `server.host`
and `server.port`, with a wildcard host (`0.0.0.0`) as `127.0.0.1`. A
manager on port 4747 writes `~/.legion/tokens/127.0.0.1_4747`, and a second
local cluster on port 4757 writes `~/.legion/tokens/127.0.0.1_4757`, so two
clusters on one laptop never present each other's token. `server.advertiseAddr`
does not change the name, because it names the manager for other machines.

If the file for this address already exists, the manager replaces it and
logs that it did. The mint only happens on a cluster with no management
token, so that file held a token from an earlier cluster at the same address
(or a lesser login to this one) that the new cluster would reject anyway.

The mint is a replicated control-lane write. In a raft cluster only the
manager that leads the control lane mints the token, at first start or when
it first wins leadership, and only that manager's data dir holds the
`bootstrap-token` file. The token itself is replicated and works against
every manager.

A manager writes the secret to `<dataDir>/bootstrap-token.pending` before the
replicated write and renames it into place after. If the manager dies between
the two, the next start finishes the rename, so a cluster can't end up with a
management token whose secret nobody holds.

### Pre-seeding the secret (infrastructure as code)

Point `acl.bootstrapSecretFile` at a file holding the secret (at least 24
characters). The manager reads it once, when it mints the initial token:

```nickel
acl = { bootstrapSecretFile = "/etc/legion/bootstrap-secret" },
```

A secret shorter than 24 characters fails the start, naming the setting.

### Turning ACLs off

`acl = { enabled = false }` is an explicit choice for a throwaway cluster.
For a quick local cluster without a config file, pass `--no-acl` instead:

```bash
legion start manager --run-worker --no-acl
```

The flag overrides `acl.enabled` for that run, with or without `-c`. It
applies only to the manager it is passed to, so in a multi-manager cluster
use the config setting on every manager instead. Every caller then has full access, including any workload that can reach the
manager: a task on the manager's own host reaches it on loopback, and in a
multi-host cluster every task can reach the address workers use. The manager
logs a warning at every start, the UI shows a banner, and `legion acl whoami`
reports `aclEnabled: false`.

## Concepts

### Resources and actions

| Resource | Covers | `read` | `write` |
| --- | --- | --- | --- |
| `job` | jobs, specs, plans, rollouts | list/inspect/plan | submit/stop/destroy/advance |
| `node` | worker nodes, pools, ledger, join tokens | list/inspect, list join tokens, capacity demand (`GetCapacityDemand`, scoped by each template's pool; it names only jobs you can `job:read`) | restart, drain/attrs, forget (`ForgetNode`), create and revoke join tokens |
| `kv` | KV entries and locks | get/list | put/patch/delete/locks |
| `state` | raw state store, saved queries | SQL reads, schema | SQL writes, saved queries |
| `operator` | raft, manager configs, observability | status/history/logs | raft commands, raft join tokens |
| `acl` | tokens, policies, users, delegated exec keys | list/get | create/update/delete |
| `exec` | exec into a running allocation | n/a | open a session, or mint an exec ticket for one (`host = true` for host-level runtimes) |
| `action` | run declared actions on an allocation | n/a | run an action (scope with `actionNames`) |
| `template` | job templates | list/inspect | publish versions, delete (scope with `templates`) |
| `scale` | task count overrides and autoscaling ([Autoscaling](autoscaling.md)) | scale status, the cluster-wide scaling inputs (`GetScalingInputs`: replicas, usage against reservations, service traffic), autoscaling status, explanations and history, the effective policies (`WatchScalingPolicies`), and a namespace's scaling defaults | `job scale` / reset / pins, and pausing autoscaling for a job (scope by job name). Separate from `job:write`, so a principal can resize without redeploy rights. The cluster's scaling defaults and the cluster-wide pause need `operator:write`. |
| `boot` | boot tickets ([Edge Boot](edge-boot.md)) | list outstanding tickets | mint a ticket (scope by job name). Separate from `job:write` for the same reason as `scale`: a platform's ticket minter can authorize individual starts without the right to redeploy the fleet. |
| `net` | network policy and plugin objects ([Network Handlers](network-handlers.md), [Network Plugins](network-plugins.md)) | list policies, show or test a job's effective policy (scope by job name); list, describe, and read the status of plugins | apply or delete a policy. Scope with `pools`, which is matched against the policy's own `selector.nodePools`, so a pool's admin can set posture for their pool without cluster-wide write. Apply or delete a plugin: a `usableBy = "policies"` plugin's `nodePools` scope it the same way; a cluster-wide one, or any `usableBy = "jobs"` plugin, needs unscoped write. |
| `snapshot` | instance snapshots ([Suspend and Resume](suspend-and-resume.md)) | list, inspect, warm a published one onto nodes, restore an instance from one, and create instances from one (restore and create also need `job:write`) | create (including `--publish`), delete, and name snapshots, suspend with a memory checkpoint, and fork (fork also needs `job:write`) (scope by job name and namespace) |
| `namespace` | namespace objects ([Namespaces](namespaces.md)) | get a namespace (listing is per item: any allow in a namespace shows it) | create, update, and delete namespaces whose names match (scope with `namespaces`). A cascade delete also needs `job:write` there. |
| `app` | [apps](apps.md): Legion's add-ons | list and show apps (scope with `names`, matched against app names), and see their views and sandboxed pages and run their actions, each also checked against the view's, page's, or action's own declared permission ([Apps: Who sees what](apps.md#who-sees-what)) | install, upgrade, uninstall, enable, disable, and change settings and config. Installing and upgrading also need every permission the manifest asks for, and a config change that touches a code field needs the app's whole grant: `app:write` alone never hands out an app's grant. Cluster-scoped. |

Actions are `read` and `write`. `write` does not imply `read`, so grant both.
Grant `state:read` sparingly, since it can read every table in the state
store, including ACL metadata. What it cannot read:

- KV values. Those are stored encrypted
  ([KV Encryption at Rest](kv-encryption.md)), so raw SQL sees ciphertext. A
  `kv` rule scoped by `paths` is the only way to read a value. That includes
  naming one in a job or net policy, which hands it to the task or the
  datapath ([KV references in a job](#kv-references-in-a-job)): a port's
  `tls.cert` or `tls.clients.ca`, an upstream's `ca`, or an upstream
  credential among them
  ([Certificates and CAs from KV](network-handlers.md#certificates-and-cas-from-kv)),
  and a network plugin object that names a signing key under `credentials`
  ([Network Plugins](network-plugins.md#signing-without-the-key)).
- Private keys, password hashes, and recorded replies.
  `signing_key.private_key_pem` (the workload-identity and boot-ticket
  signing keys), `net_mesh_ca.key_pem` (the built-in cluster CA key),
  `acl_user.password_hash`, and `idempotency_record.response_message` (a
  keyed `boot-ticket create` records the whole ticket, so a retry gets it
  back) always read as `NULL` through raw SQL, however the query reaches
  them. A `WHERE` clause on
  them matches nothing, and `INSERT ... SELECT` copies `NULL`. With a `file`
  or `exec` [cluster CA provider](cluster-ca.md), the CA key is not in the
  store at all.

`state:write` is still equivalent to full control of the cluster: it can
rewrite any row, including ACL tokens and policies. State backups
(`GetStateBackup`, `operator:write`) are the whole database, keys included.

Grant `snapshot:write` as carefully as read access to a job's secrets. A memory
snapshot captures everything the workload had in memory, including secrets,
tokens, and decrypted KV values, so `snapshot:write` on a job is equivalent to
reading every secret that job has held in memory. That is why snapshots are a
resource of their own rather than part of `job:write`.

Suspend follows the same line. `legion instance suspend` (`SuspendInstance`)
needs `job:write` on the instance's job, like `instance stop`, plus
`snapshot:write` when the checkpoint is `memory`, because a memory checkpoint
holds the workload's memory. An explicit `--checkpoint memory` is checked
before the instance is looked up. Resuming (`instance start`) needs
`job:write` only: it restores the instance's own checkpoint and widens nothing.

Fork (`legion instance fork`, `ForkInstance`) needs `job:write` and
`snapshot:write` on the origin's job, whatever the checkpoint kind: every
fork starts with what the origin had in memory or on disk. Creating instances
from a snapshot (`instance create --from-snapshot`) needs `job:write` and
`snapshot:read`, since it reads the snapshot. Both grants are checked before
anything is looked up. Naming a snapshot (`legion snapshot alias`) needs
`snapshot:write`.

### Policies

A policy is a named JSON array of rules:

```nickel
[
  { resources = ["job"],  actions = ["read", "write"], namespaces = ["default"], names = ["web-*"] },
  { resources = ["kv"],   actions = ["read"],          paths = ["team-a/*"] },
  { resources = ["node"], actions = ["read"],          pools = ["gpu-*"] },
  # explicit deny always wins, across all attached policies:
  { resources = ["*"],    actions = ["*"], effect = "deny", paths = ["team-a/secrets/*"] },
]
```

- `resources` / `actions` accept `*`.
- `effect` is `allow` (default) or `deny`.
- Scope selectors narrow a rule with `*`-globs: `namespaces`
  ([Namespaces](namespaces.md)), `names` (bare job names), `pools`
  (node pools), `paths` (KV keys), `actionNames` (declared actions),
  `templates` (job-template names), and `bands` (job priority bands). A rule
  with a selector only matches requests carrying that attribute.
- **A rule over a namespaced resource** (`job`, `exec`, `action`, `scale`,
  `boot`, `snapshot`, `namespace`) **must carry `namespaces`**, and a rule
  over only cluster-scoped resources (`node`, `kv`, `state`, `operator`,
  `acl`, `template`, `net`, `app`) must not. `namespaces = ["*"]` covers every
  namespace, including ones created later. A rule mixing the two kinds is
  refused, as is an empty selector; `resources = ["*"]` rules and deny rules
  are exempt. A rule naming an exact namespace that does not exist yet is
  accepted with a warning. See [Namespaces, ACLs](namespaces.md#acls).
- List endpoints filter to what the caller can read: a token scoped to
  `paths = ["team-a/*"]` sees only `team-a/` keys in `legion kv list`.
- **Reserved names skip `*`.** In an allow rule, a `namespaces` glob reaches
  a reserved `legion-*` namespace (Legion's own `legion-system`) only when
  the pattern itself starts with `legion-`, a `pools` glob reaches a
  reserved `legion-*` pool (the managers' system workers, pool
  `legion-system`) only when it starts with `legion-`, and a `paths` glob
  reaches a key under `legion/` only when it starts with `legion/`, the way
  a shell's `*` skips dotfiles. A rule with no `paths` or no `pools`, and a
  `resources = ["*"]` rule with no `namespaces`, reach what `*` reaches. So
  `namespaces = ["*"]`, `pools = ["*"]`, and `paths = ["*"]` cover every
  tenant namespace, pool, and key but not what Legion runs for itself: a
  `node` rule with `pools = ["*"]` (or none) does not list, cordon, or
  drain a manager's system worker, or mint a join token into its pool. "Everything"
  is `["*", "legion-*"]` and `["*", "legion/*"]`. Deny rules keep plain globs (a deny on `*` still
  denies inside `legion-system`), and management tokens bypass rules. A
  denial inside a reserved name says so in its hint. See [Namespaces,
  Reserved namespaces](namespaces.md#reserved-namespaces).

**Restricting an account to specific job templates.** A rule with a selector
never matches a request that lacks that attribute. So a `job` rule carrying
`templates` grants submission only through those templates, and raw
`legion job run` stays denied:

```nickel
[
  { resources = ["template"], actions = ["read"], templates = ["render"] },
  { resources = ["job"], actions = ["read", "write"], namespaces = ["default"], templates = ["render"] },
]
```

The account can run `render`, and see and stop the jobs that template
created, but cannot submit an arbitrary spec. See
[Job Templates](job-templates.md) for the full model, including the
collision guard and which endpoints carry the template attribute.

**Restricting which priority bands an account may claim.** A job's
`priority` band decides what it can displace
([Preemption and Eviction](preemption-and-eviction.md)). If anyone can use the
top band, everyone will, and it stops meaning anything. Use the `bands`
selector on `job` rules to control this:

```nickel
[
  # owners of prod-* jobs may claim anything, including the never-preempted top band
  { resources = ["job"], actions = ["write"], namespaces = ["default"], names = ["prod-*"],
    bands = ["critical", "high", "elevated", "normal", "low", "background"] },
  # everyone else tops out at `high`
  { resources = ["job"], actions = ["read", "write"], namespaces = ["default"],
    bands = ["high", "elevated", "normal", "low", "background"] },
]
```

Keep in mind:

- A rule that **omits** `bands` covers every band. Bands are open by default
  and only become a privilege where a policy says so.
- An **unbanded** job is a `normal` job. The submit path fills in the default
  band before enforcing, so a `bands` policy still matches ordinary
  submissions instead of silently granting nothing.
- The check also applies to submitting **through a template** that renders a
  band, so templates are not a way around it.

A refused submit names the band:

```console
$ legion job run api.ncl   # priority: 'critical'
error: permission denied: this token's policy does not allow submitting jobs
in the `critical` priority band (see the `bands` selector in
docs/auth-and-acls.md)
```

Bands are not gated on `operator:write`. `operator` covers raft, manager
configs, and observability. Gating bands on it would make everyone who may
submit a critical job a raft administrator, and it could not express "team A
may use `critical` for their jobs".

#### Host-level jobs

Some tasks run on the host as root, or close to it. Submitting or updating a
job with one of them needs a `job:write` rule with `host = true`, the same
flag `exec` rules use for exec into host-level runtimes:

| Task | Host-level when |
| --- | --- |
| `exec` | always: it is a process on the host, root unless it sets `user` |
| `container` | `network.mode = "host"`, `security.userNamespace = false`, or `security.seccomp = "unconfined"` |
| `sandbox` | `hypervisor = "dev"` (a container standing in for a VM, not a security boundary) |
| a runtime plugin other than `noop` | always: Legion cannot vouch for its isolation |
| any runtime | an `exec` health check: the worker runs its command on the host |
| any runtime | an artifact or `image` is a `file://` source or a `git::file://` repository: the worker copies node files into the task as root |
| any runtime | an artifact sets `auth.nodeIdentity`: the fetch presents the worker node's own credential to the source ([Artifacts](artifacts.md#workload-identity-auth-pending)) |

```nickel
# Deploy containers and sandboxes, but nothing that runs on the host:
{ resources = ["job"], actions = ["write"], namespaces = ["default"], names = ["team-a-*"] },
# Platform admins may deploy host-level jobs too:
{ resources = ["job"], actions = ["write"], namespaces = ["default"], names = ["*"], host = true },
```

A refusal names the task and why, and only when the host grant is what is
missing:

```console
$ legion job run backup.ncl
error: permission denied: job "backup" has a host-level task ("dump" uses runtime "exec").
Submitting it needs a job:write rule with host = true (docs/auth-and-acls.md, "Host-level jobs")
```

Management tokens hold every grant, so the first-start token and single-box
flows are unaffected. The check runs on every path that submits a spec
(`RunJob` and `RunJobTemplate`), after the spec parses. Scaling, instances,
and stopping a job that already exists don't change what runs, so they need
only `job:write`. A stored `require` policy that imposes `mode = "host"`
makes every job it matches host-level; writing one needs `net:write`, which
is already an admin grant.

#### KV references in a job

Naming a KV key in a job is reading it: the value reaches the task (or the
datapath, or an artifact source), so submitting the job needs `kv:read` on
the key, on top of `job:write`. That covers every place a job can name one:

- `$path/key` values anywhere in the spec,
- `kvInjections` env and files, the `{{ kv "…" }}` lookups in their
  templates, and a `templateKv` pointer,
- an artifact's `auth.kv` (a path filled in by a cluster `artifacts.auth`
  matcher needs no grant),
- a port's `tls.cert` or `tls.clients.ca`, an upstream's `ca`, and
  `connect.upstreams[].credentials` (the
  [Sources](network-handlers.md#sources) the datapath fetches).

`job run`, `job plan`, and template renders and runs check it before anything
is placed:

```console
$ legion job run web.ncl
error: permission denied: ... (the jobspec reads kv secrets/db-password; the submitter needs kv read on it)
```

The check is made when the job is submitted. A template stored in KV
(`templateKv`) is re-read when it changes, but a re-render may only read
keys the job already names: an edit that adds a lookup is not rendered, the
job keeps its last render, and a `job.kv_render_refused` event names the
new keys. Resubmitting the job checks them against whoever submits it.
`kv:write` on a `templateKv` key still decides what the job renders from
the keys it names.

#### Namespaces

Jobs, and everything scoped by a job, live in [namespaces](namespaces.md).
A team's whole policy is usually one glob:

```nickel
[
  { resources = ["job", "exec", "action", "scale", "snapshot"], actions = ["read", "write"],
    namespaces = ["payments-*"] },
  # Delegated creation: payments may create and delete payments-* namespaces.
  { resources = ["namespace"], actions = ["read", "write"], namespaces = ["payments-*"] },
]
```

`names` then narrows by the job's bare name inside those namespaces. A
tenant admin's `namespaces = ["*"]` does not reach `legion-system`: a token
that could submit to or exec into Legion's own jobs could borrow the
credentials they run with, so that reach is always written out
(`["*", "legion-*"]`).
`ListNamespaces` shows a namespace to anyone holding any allow there, and
`WhoAmI` reports the set as `readable_namespaces`. A denial on a namespaced
resource names where the token can act (`Hint: this token can write job in
payments-*`), read off the caller's own rules; it never names the target, so
a denial cannot confirm that something exists. Namespace policies (quota,
pools, runtimes) are written with `acl:write`, not `namespace:write`, so a
namespace's writer cannot raise its own limits.

Manage policies with `legion acl policy apply|list|get|delete`. A policy file
is Nickel (`.ncl`) or JSON (`.json`), read by the same rules as every other
Legion config (see [Configuration Language](configuration-language.md)). It
can be a bare rules array or `{description, rules}`. `apply` prints a
warning for each rule naming an exact namespace that does not exist yet.
Policy names starting with `grant:` are reserved for [job grants](#job-grants).

### Job grants

A job can ask for access to Legion itself: `permissions` (inline rules, the
same shape as a policy) and `policies` (policy names) on the reserved
`legion` identity of a task ([Workload identity:
Permissions](workload-identity.md#permissions)). The job's workload token then
carries them, and its task reaches the API through a local socket with no
token handling.

**You can grant only what you hold.** Every path that stores a job config
(`RunJob`, `RunJobTemplate`) checks that the submitting token holds every
requested rule, and every rule of every named policy (which must exist).
It runs on every submit, an unchanged one included. Management tokens pass;
in audit-only mode a miss is logged under `acl_audit` and allowed. A miss
names the first rule not held:

```text
cert-renewer-app-example-com: identity 'legion' asks for kv:write on certs/*, which your token does
not hold
Hint: have someone with that access submit this job, or ask for it to be granted to you
```

**What "holds" means.** Deciding exactly whether one set of glob rules
contains another is set algebra; the check decides a sufficient condition
instead, which is sound and occasionally conservative:

- A requested `(resource, action)` is held when one allow rule covers the
  same pair and every one of its selectors covers the requested one. A
  requested `*` resource or action asks for each one.
- A requested glob is covered when the requested pattern, as a string,
  matches a held glob: a held `pay*` covers a requested `payments-*`, and a
  held `certs/*` covers `certs/app.example.com`. For `*`-only globs that is
  exact: every name the requested pattern reaches, the held one reaches too.
- **Reserved names first.** A requested pattern that can reach a reserved
  name (it starts with `legion-` for namespaces and pools, or `legion/` for
  KV keys) is covered only by a held pattern with the same prefix. A held `*` does not
  reach `legion-system`, so it does not cover a requested `legion-*`:
  without this, a tenant admin could grant a job `exec:write` into Legion's
  own jobs. The error says so when this is why.
- A selector the request leaves out reaches more than any held selector, so
  it is covered only by a held rule that leaves it out too (a held
  `names = ["*"]` does not cover a request with no `names`).
- `host = true` on a request needs `host = true` on the held rule.
- **Deny rules.** When the submitter holds any deny rule on a resource, a
  grant on that resource also needs `acl:write`: subtracting denies is not
  attempted, and `acl:write` can write any policy anyway.
- A requested deny rule only takes access away, and is never checked.

Conservative cases have someone whose own policy covers the request outright
submit the job: a request covered only by two held rules together (`a*` and
`b*` for `*`), or a cluster-scoped resource requested from a held rule that
names `namespaces` (which never matches a cluster-scoped request).

**Grants are per version and revocable.** The grant is recorded with the job
config version it was checked for, in the submitter's name, and resolved
every time the job's token is verified:

```console
$ legion acl grants list
 job                          | version | grants                     | granted_by | granted_at           | revoked
------------------------------+---------+----------------------------+------------+----------------------+---------
 cert-renewer-app-example-com | 1       | kv:read,write on certs/*   | ops-alice  | 2026-10-28T10:02:11Z | -

$ legion acl grants revoke cert-renewer-app-example-com
revoked 1 grant of job `cert-renewer-app-example-com`; its tokens lose them at their next request
```

`list` takes `--job` (with `-n` for a namespaced job) and `--all` to include
revoked grants; `revoke` takes `--version` to revoke one version only and
`--idempotency-key` for retries; both take `--json`. `ListGrants` needs
`acl:read` and `RevokeGrant` `acl:write`. A revoke applies at the workload's
next request, including ACL tokens minted from its identity with `legion login
--method legion`, which carry a `grant:alloc/<id>` reference rather than a
copy. The next submit of the job by someone who holds the access grants it
again. A grant outlives the granter's own access, as in Kubernetes RBAC:
revoke it when that matters.

### App grants

An [app](apps.md)'s job carries a grant the same way a job does, through the
`permissions` of its `legion` identity, but the grant is recorded on the app
record when the app is installed or upgraded, and checked against whoever
does it (`InstallApp`, `UpgradeApp`), with the coverage rules above. The app
reconciler's submits of the app's job are then checked against that recorded
grant, so they keep working after the installer's own access changes; anyone
else submitting the app's job directly still needs everything it asks for. A
`SetAppConfig` that changes one of the manifest's code fields re-runs the
check against the caller. Bundled apps' grants are recorded at seed time by
`legion:bootstrap`, the one grant that is not checked against a person.

```text
Error: app gitops 0.2.0 asks for job:write in team-*, which your token does not hold
Hint: have someone who holds it do this, or ask for it to be granted to you
```

**The app plane.** Two RPCs are for the bundled autoscaler alone:
`ReportScalingStatus` (its heartbeat and per-task state) and
`RecordScalingAction` (a dry-run policy's "would" rows). They take no ACL
rule; instead they accept only a credential whose job grant belongs to an
allocation of `legion-system/autoscaler` (its `legion` token, or an ACL token
minted from it), or a management token. Anyone else gets `PermissionDenied`
(`Unauthenticated` without a credential), so a principal with `scale:write`
cannot forge the autoscaler's status. `ScaleTask`'s `source = "autoscaler"`
is honored on the same test ([autoscaling.md](autoscaling.md#access-control)).

### App views and the viewer assertion

An app's grant is the app's power, not its viewers'. A button on an app's
panel is pressed by a viewer who may hold much less, so the manager
authorizes every view and action **as the viewer** before the app is asked:
`app:read` on the app, then the permission the view or action declares,
checked on the job, task, or node it is shown for. Only then does it forward
the request to the app, with a **viewer assertion**: a 60-second JWT signed
with the workload-identity key, `aud = app:<name>`, naming the viewer, the
permission checked, and the context. The app verifies it and acts only on
that context, so a viewer can press a button only where they hold the
permission the manifest declares for it; that the declaration fits what the
action does is part of what an installer approves. Every action is recorded
as an `app.action` event. [Apps: Views and actions](apps.md#views-and-actions) has the details.

An app's **sandboxed page** is fetched the same way (`GetAppPage`: `app:read`
on the app, then the page's declared permission, then a viewer assertion
for the page), and then runs in the browser, in an opaque-origin iframe that
holds no token. It reaches Legion only through the SPA, which makes each
call **as the viewer** with the viewer's own token: only the RPCs the page's
manifest lists, with every write confirmed by the viewer first. The SPA
stamps those calls `legion-app-page: <app>/<page>`, and the manager refuses
one whose RPC the page's current manifest does not list. That header narrows
what a page may ask for; it never widens what the viewer may do, which the
RPC's own ACL check decides as for any call. A page can still read what its
viewer can read through its RPCs and send it elsewhere
([Apps: What the sandbox does not stop](apps.md#what-the-sandbox-does-not-stop)).

### Tokens

Tokens are opaque secrets (`lgn_…`), and the manager stores only a SHA-256
hash. Each token has:

- a public **accessor ID**, which is safe to log and used for revocation
- a type: `management` (full access), `client` (policy-scoped), or `worker`
  (node credential)
- attached policy names
- an optional expiry

Create, inspect, and revoke tokens with `legion acl token create|list|delete`.
Only a management token can create another management token.

To bound token lifetimes, set `acl = { tokenMaxTtl = "720h" }`. Every client
token (created or login-minted) must then expire within the bound. Management
tokens are exempt. Expired ACL and join tokens are rejected at
authentication time and pruned hourly.

Long-lived streams (`StreamEvents` and exec sessions, including the UI's exec
WebSocket) re-check their credential every 30 seconds. Revoking or expiring a
token, or removing the policy that granted the stream, closes the stream
within that interval with the same `Unauthenticated` or `PermissionDenied`
error a new request would get. A [direct exec](exec-and-actions.md#direct-exec)
session never touches a manager after its ticket is minted, so its worker
reports it on every heartbeat instead: the manager re-checks the minting
token there, and the session ends within one heartbeat interval of the token
being deleted, expiring, or losing the grant.

For infrastructure-as-code, `acl.bootstrapSecretFile` pre-seeds the initial
management token's secret (see [Pre-seeding the
secret](#pre-seeding-the-secret-infrastructure-as-code)).

Clients send the secret as a bearer credential. The CLI checks the
`LEGION_TOKEN` env var first, then the token file for the manager it dials,
`~/.legion/tokens/<host>_<port>`. The name comes from `LEGION_ADDR` (default
`http://127.0.0.1:4747`) with the scheme dropped, so `http://10.0.1.5:4747`
reads `~/.legion/tokens/10.0.1.5_4747`. `legion login` writes that file and
`legion logout` removes it, both for the address in `LEGION_ADDR`.
`LEGION_TOKEN_FILE` names one file for every address instead. The UI stores
the secret in browser localStorage via the **Access** page.

A token works against every manager of its cluster, but the file is per
address. To use a raft cluster through several managers, run `legion login`
against each address, copy the file to each address's name, or set
`LEGION_TOKEN`.

When the manager answers `Unauthenticated`, the CLI's hint names the
credential it sent, because a revoked token and another cluster's token look
the same from the manager's side:

```text
Error: failed to list jobs: unknown or revoked ACL token [code=Unauthenticated]
Hint: the CLI sent the token stored for http://127.0.0.1:4757 in
/home/me/.legion/tokens/127.0.0.1_4757, which this manager does not accept
(revoked, expired, or issued by another cluster); run `legion login`, set
LEGION_TOKEN, or on the manager's machine use <dataDir>/bootstrap-token
```

With no token at all, the hint names the file the CLI looked for. The `--json`
envelope carries the same text in `error.hint`.

### Logging in (userpass, JWT, OIDC / SSO)

Every identity source is an **auth method**. Each one verifies a credential
and returns a principal plus policy names. The manager then mints an ACL
token named `<method>:<principal>` (for example `userpass:alice`), so audit
logs always show how an identity was established. Built-in methods:

- **Username/password** (`userpass`, always available): create a user with
  `legion acl user upsert alice --password ... --policy dev-team`
  (the password is argon2id-hashed). Then `legion login -u alice -p ...`
  mints a token with the user's policies (TTL: `acl.loginTtl`, default 24h).
  The UI Access page has the same form.
- **JWT / OIDC tokens**: configure an auth method in the manager config. Then
  either present the externally issued JWT directly as the bearer credential
  (validated on every request) or exchange it once with
  `legion login --method corp-oidc --jwt-file token.jwt`.
- **Single sign-on (browser flow)**: for `oidc` methods with an
  `oidcClientId` configured, `legion login --method corp-oidc` opens the
  browser for the standard authorization-code + PKCE flow. The loopback
  callback listens on `127.0.0.1:4749` (override with `--callback-port`), so
  register `http://127.0.0.1:4749/oidc/callback` as a redirect URI at the
  IdP. The UI Access page offers the same flow as a "Continue with <method>"
  button (redirect URI `<ui origin>/oidc/callback`). Binding rules map
  verified claims (for example `groups`) onto policies.
- **Webhook**: `type = "webhook"` hands verification to your own HTTP
  endpoint. Use it to integrate LDAP, SAML bridges, or custom identity
  systems in any language. Legion POSTs
  `{"method", "username", "password", "jwt"}` and expects
  `{"principal": "...", "policies": ["..."]}` on HTTP 200 (401 = bad
  credentials, 403 = verified but denied). Webhooks are only called at login,
  never on every request.

A `jwt` method verified by keys (`jwksUrl`, `jwksJson`) and every `oidc`
method must set `boundAudiences`, or the manager refuses to start: an issuer
mints tokens for many services, and without an audience any of them would
log in here. An `oidc` method's `oidcClientId` is its audience when
`boundAudiences` is unset. `jwksUrl` and `issuer` must be `https://` unless
they point at loopback. `hmacSecret` methods are exempt from the audience
rule, since only the secret's holder can mint for them.

When you embed Legion as a library, you can also register custom identity
sources through the same `AuthMethod` interface
(`Authenticator::register_method`). See [docs/plugin-system.md](plugin-system.md).

```nickel
acl = {
  enabled = true,
  authMethods = [
    {
      name = "corp-oidc",
      type = "oidc",                      # or "jwt" with jwksUrl/jwksJson/hmacSecret
      issuer = "https://idp.example.com",
      boundAudiences = ["legion"],
      userClaim = "email",
      bindingRules = [
        { claim = "groups", value = "platform", policies = ["ops"] },
        { policies = ["base-read"] },     # unconditional
      ],
      tokenTtl = "8h",
    },
  ],
},
```

### Anonymous policy and audit mode (migration)

ACLs are on for a new cluster. For a cluster that ran with
`acl.enabled = false`, there are two ways to turn them on without breaking
anyone (the first start with ACLs on mints the initial management token as
described above):

1. **Audit-only**: with `acl = { enabled = true, auditOnly = true }`, every
   denial is logged but allowed. Logs use the tracing target `acl_audit` and
   include principal, accessor ID, resource, action, and scope. Watch the
   log, write policies, then drop `auditOnly`.
2. **Anonymous policy**: a policy named `anonymous` applies to requests with
   no credential. Grant it temporary read access while you distribute
   tokens, then delete it.

### Worker nodes: join tokens and node tokens

Adding a worker takes two commands:

```bash
# On the manager (or any host with node:write access):
legion join-token create --type default --ttl 24h
# On the worker host (paste the printed token):
legion join lgnjoin_...
```

Pasting the token is fine for a first try. For a worker that keeps running,
pass it in a file instead: a token on the command line stays in the
process's argv, which any local user can read with `ps` or
`/proc/<pid>/cmdline`, and the worker warns about it.

```bash
legion join-token create --type default --ttl 24h -j | jq -r .data.token \
  | sudo install -m 0600 /dev/stdin /etc/legion/join-token
legion join --token-file /etc/legion/join-token
```

`legion start worker --token-file <PATH>` takes the same flag. There is no
environment variable for the token, because exec tasks inherit the worker's
environment. See
[Running a worker under a service manager](architecture-managers-workers.md#running-a-worker-under-a-service-manager)
for a systemd unit.

The join token bundles the manager address, the worker type, and a random
secret, so the worker needs no other configuration. The manager stores only
a hash of the secret. List and revoke join tokens with
`legion join-token list|delete` or from the UI's Access → Worker Join tab.
Set `server.advertiseAddr` when workers reach the manager at a different
address than it binds. A manager bound to every interface (`host =
"0.0.0.0"`) with no `advertiseAddr` puts `127.0.0.1` in its join tokens,
which no other machine can use: the worker dials its own loopback and
retries, and nothing on the manager looks wrong. The manager warns about it
at startup and with every token it mints in that state (`legion join-token
create` prints the warning on stderr, and `--json` carries it as
`data.warning`).

Workers get their attributes from their worker type's template (pool,
attributes, and resources from `workerConfigPath`) plus worker-side
fingerprinting. The join token pins which template applies.

#### Limiting a token's uses

`--max-uses N` makes a token admit at most N new nodes. Use it whenever the
number of machines is known when the token is minted: a fleet bring-up, or a
node group asking its provider for N machines:

```bash
legion join-token create --type default --ttl 1h --max-uses 3
legion join-token list
# ID        WORKER_TYPE  DESCRIPTION  USES  ATTRIBUTES  EXPIRES_AT
# 3f9c2a1b  default                   2/3               2026-10-28T13:00:00Z
```

- Each new node that registers with the token uses it once. A spent token
  refuses the next new node with `PermissionDenied: join token <id> is spent:
  3 of its 3 uses (max_uses) are taken`.
- A node re-registering under its own name is not a new use: a restart that
  falls back to the join token, or a node that went `lost` and rejoins. (A
  restart with the node token never touches the join token at all.)
- A node removed with [`legion node forget`](architecture-managers-workers.md#forgetting-a-node)
  is new again, so a spent token cannot bring a forgotten machine back.
- The limit holds with ACLs off too, for a worker that presents the token.
  `--max-uses 0` is refused; leave the flag out for no limit.

#### Node attributes from the join

A node's config attributes come from three places, and the more specific one
wins:

1. the join token's attributes (`legion join-token create --attr k=v`,
   repeatable), stamped on every node that joins with it;
2. the host's own `--attr k=v` on `legion join` or `legion start worker`
   (repeatable), for what only that machine knows;
3. the worker type's `attributes`.

```bash
# The minter (an operator, or whatever provisions the group) says which group:
legion join-token create --type default --max-uses 5 \
  --attr legion.nodegroup=workers-general
# The host says which machine it is:
legion join --token-file /etc/legion/join-token \
  --attr legion.provider-id=$(curl -s http://169.254.169.254/latest/meta-data/instance-id) \
  --attr rack=r12
```

Constraints, `distinct`, and `legion node info` see the merged attributes.
The token's attributes survive the node's restarts: they are recorded on the
node as `legion.join` (`{"token": "<id>", "attributes": {...}}`) and applied
again whenever the node re-registers with its node token.

Keys under `legion.` are reserved. With ACLs enforced, a worker cannot report
one itself: a `legion.*` attribute reaches the node only if the join token
sets it, the worker type sets it, or (on a restart) the node's own record
already has it. One the worker reports that nothing vouches for is dropped
with a warning, so a host cannot claim `legion.nodegroup = gpu-expensive` and
attract that group's work. `--attr` may set only the per-host reserved keys,
today `legion.provider-id`; any other `legion.*` key, and any key Legion
reads as node structure (`namespaces`, `restriction`, `resources`,
`reservedResources`, `maxAllocations`, `capacity`, `oversubscribe`,
`workerAddress`, ...), is refused with `InvalidArgument` (the CLI checks
before it starts the worker). A token may set any `legion.*` label except
`legion.role` and `legion.join`, which Legion writes itself, and no
structural key. Keys are at most 128 bytes, values 1024, and a token or a
registration carries at most 64.

`legion.provider-id` is a claim the host makes about itself, like every
`--attr`. Anything that acts on a machine by that id (a script that
terminates instances, say) should check it against the provider's own list
of instances first.

When ACLs are enforced:

- `WorkerPreRegister`/`WorkerRegister` require a valid join secret, or the
  node's own node token (see below).
- Registration with a join secret mints a per-node **node token** (returned
  in the reply) that the worker attaches to every worker-plane call. Node
  tokens grant nothing on the user-facing API, and they do not expire.
- The worker persists its node token in its state directory
  (`$LEGION_DATA_DIR/workers/<node>/node_token`, mode `0600`, in a directory
  only the worker's user can enter) and is never logged. A restarted worker
  re-registers with that node token instead of its join secret, so **a join
  token is only needed to introduce a node**: once the node has joined, its
  join token can expire or be revoked and the node still restarts, reboots,
  and upgrades normally.
- A join token only admits its own worker type. Pre-registering as another
  type, or registering into a pool other than that type's pool, fails with
  `PermissionDenied`.
- A node token only speaks for its own node. Every worker-plane RPC that names
  a node (`Heartbeat`, `HealthCheck`, `FetchDatapathMaterial`,
  `GetArtifactCredential`, `RatifyClaim`, and the identity and certificate
  issuers) refuses a node id that does not match the token with
  `PermissionDenied`. A node token can re-register its own node in its current
  pool, but not any other node. Management tokens act for any node.
- Registering with a join secret rotates the node token (the old one stops
  working). Re-registering with the node token keeps it: the reply carries no
  new token, so a crash during a restart can never leave the worker holding
  a revoked one.
- If the manager refuses the persisted node token (the node was removed or
  forgotten, or ACL state was reset), or a heartbeat comes back `Unauthenticated`, the
  worker discards it and registers with its join secret. If that is gone
  too, the worker exits with `Unauthenticated: unknown or revoked join
  token`; mint a new join token for that host and run `legion join` again.
- A join token introduces a new node, or rejoins one whose node has gone
  `lost` (a host that lost its disk). It cannot take over a node that is
  still live: that registration fails with `PermissionDenied`, naming the
  node and its status, unless the request also carries that node's own
  token (a restart). Every node of a worker type holds the same join secret,
  so without this any of them could register as a running peer. A
  compromised worker can still claim the name of a node that is down. Treat
  join secrets like node credentials: give them short TTLs (minutes to a
  day, long enough to bring the hosts up) and revoke them once the fleet has
  joined. Restarts do not need them. Treat the node token file like the
  node's private key: it lets its holder act as that node.
- A registration that claims a simulator fleet (the `workerFleet`
  attribute, which earns one certificate for every node of the fleet) is
  refused with `PermissionDenied` unless the join token's worker type grants
  that fleet in its own `workerFleet` attribute, and a node token cannot
  move its node to another fleet
  ([worker-simulation.md](worker-simulation.md)).
- A node's certificate is issued for `<label>.node.legion`. A name that is
  already a plain DNS label (lowercase letters, digits, single `-`) is its
  own label; any other name (`ip-10-0-1-7.ec2.internal`, `Node_1`) gets a
  readable prefix and `--` plus 16 hex digits of the name's SHA-256, so two
  different names never share a certificate name.
- In `auditOnly` mode, workers without join tokens are admitted but logged,
  and so are the worker-type, pool, and node-id mismatches above. The two
  identity-token issuers refuse a foreign node id even in audit-only mode.
  Use this to migrate fleets that predate ACLs: enable audit mode, restart
  workers onto join tokens at your own pace, then enforce.

Browsers are also limited by origin, with or without ACLs: the gRPC-Web API
and the exec WebSocket only answer loopback origins and those listed in
`server.allowedOrigins` (see
[UI Architecture](ui-architecture.md#allowed-browser-origins)). With ACLs off
this is the only thing that stops a web page from driving a local manager.

### The worker plane

Managers reach workers on the worker plane (port 4848) for dispatch, exec,
logs, and restart. It is authenticated independently of ACL mode, because a
manager's private key never leaves the managers:

- The listener is always mutual TLS with the node's certificate from the
  cluster CA.
- `PostWork`, `GetTaskLogs`, `RestartWorker`, `StopSessions`, and
  `ForwardAppRequest` admit only a caller that presents a manager certificate
  (`legion://manager/<id>`). `ExecTask` admits
  a manager, or a holder of an exec ticket on a worker that opts in to
  [direct exec](exec-and-actions.md#direct-exec). `StartClaim` admits a holder
  of a valid boot ticket.
- Managers verify the worker they dial against `<node>.node.legion`.
- Join tokens pin the cluster CA (`caHash`), and a joining worker refuses a
  manager whose CA does not match.
- Node keys are generated on the node. `IssueNodeCertificate` takes a CSR
  only and never returns a private key. Managers generate their own keys the
  same way and have a CSR signed.
- The cluster CA is built in by default. It can instead be an intermediate
  from your own PKI on each manager's disk (`clusterCa.provider = "file"`)
  or an external signer such as Vault PKI (`"exec"`), in which case its key
  never enters the state store. Legion always chooses the names, and checks
  every certificate such a provider returns before handing it out. See
  [The Cluster CA](cluster-ca.md).

See [Managers and Workers](architecture-managers-workers.md#the-worker-plane)
for the caller-class table and the ports, and [Rotating the cluster
CA](#rotating-the-cluster-ca) below.

The worker's own connection to the manager (registration, heartbeats, the
JWKS) is always TLS, verified against the same CA: see
[Workers reach managers over TLS](#workers-reach-managers-over-tls) below.

### Rotating the cluster CA

One CA per cluster signs node, manager, and mesh certificates
(`legion net ca show` prints the provider, the trust bundle, its pin, and
the rotation phase). The built-in CA rotates in two steps that break no
connection:

```console
$ legion net ca rotate
Rotated the cluster CA. The new CA (pin sha256:...) signs every certificate from now on.
The previous CA stays trusted until 2026-11-03T18:05:11Z, so no worker-plane or mesh connection breaks;
nodes re-issue their certificates within one heartbeat. Join tokens minted before
the rotation work until then; mint new ones after.
```

1. The new CA is generated and cross-signed by the old one, and signs every
   certificate from then on. Chains carry the cross-signed certificate, so a
   holder that still trusts only the old CA keeps verifying.
2. The old CA stays in every trust bundle for one certificate lifetime (24
   hours). Every node sees the new CA's pin on its next heartbeat reply and
   re-issues at once.
3. After the overlap, the old CA leaves the bundle. Join tokens that pin it
   stop working; mint new ones. A worker that was offline for the whole
   overlap must re-join.

A CA from a `file` or `exec` provider rotates outside Legion, and
`legion net ca rotate` refuses with `FailedPrecondition` and the procedure:
add the new CA to the trust bundle, wait a heartbeat, switch the signing CA,
and remove the old CA a day later. Each step reaches every worker within one
heartbeat, because heartbeat replies name the manager's whole trust bundle.
See [The Cluster CA](cluster-ca.md#rotation).

### TLS

The manager listener serves TLS when configured:

```nickel
server = {
  tls = { enabled = true, certPath = "/etc/legion/server.pem", keyPath = "/etc/legion/server.key" },
},
```

Clients use `https://` in `LEGION_ADDR` (CA configurable via
`LEGION_TLS_CA_CERT`). Join tokens minted by a TLS-enabled manager advertise
an `https://` address automatically.

The exec WebSocket listener (`server.port + 1`, used by the UI terminal and
actions) serves TLS with the same certificate and key. The telemetry
listener (`/metrics` and health probes, `server.port + 2` by default) stays
plaintext whatever `server.tls` says.

#### Workers reach managers over TLS

Workers always speak TLS to the manager's API port, whether or not the
operator turned `server.tls` on. Registration, heartbeats (whose replies
carry rendered KV secrets and identity tokens), artifact credentials, and
the JWKS that boot and exec tickets are verified against all ride it.

- The worker sends the name `manager.legion`, and the manager answers that
  name with its cluster server certificate (`manager.legion`,
  `legion://manager/<id>`), signed by the cluster CA, followed by any
  intermediates and the trust bundle. Any other name gets the operator's
  `server.tls` certificate when one is configured.
- The worker verifies that certificate against its join token's CA pin from
  the first byte of the first connection, before it sends its join secret,
  and against the cluster CA bundle once it holds one. A manager whose CA
  does not match is refused with the same message as a pin mismatch at
  join.
- Without `server.tls`, the manager tells TLS from plaintext by the
  connection's first byte (a TLS handshake starts with `0x16`, HTTP never
  does), so the CLI and the UI keep using `http://` on the same port while
  every worker uses TLS. With `server.tls` on, a plaintext connection is
  dropped.
- A join token keeps the address the manager advertises, `http://` or
  `https://`. The worker dials TLS to that host and port either way.

#### Managers reach each other over TLS

Raft traffic between managers (the log, snapshots, joins, and the writes and
requests a follower forwards to the leader, which carry the caller's
token) rides TLS on the same API port. Every manager derives one key from the
cluster secret it already holds (a joining manager gets the secret in its
raft join token) and answers the name `raft.legion` with it; a peer accepts
that key and nothing else, so a manager that answers proves it holds the
secret before the secret is sent. The secret in each request still
authenticates the caller. This works whether or not `server.tls` is on, and
needs no certificates from the operator. Every manager of a cluster must run
a version with it: a manager from before it cannot talk to one after.

### Introspection

`legion acl whoami` (CLI), `WhoAmI` (gRPC), the UI Access page, and the
`legion_acl_whoami` MCP tool all report whether ACLs are enabled, how the
caller was authenticated, which policies apply, and which namespaces the
caller can read (`readable_namespaces`). Start here when
debugging a `PermissionDenied`.

### Recovery

If you lose every management token's secret, delete the management rows from
the `acl_token` table in the manager's SQLite store (on the control-lane
leader in a raft cluster; this requires filesystem access to the manager
host), then restart the manager. With no management token in the store, it
mints a fresh one at start and writes it to `<dataDir>/bootstrap-token`
exactly as at first start. Use `acl.bootstrapSecretFile` to choose the
secret.

There is no `legion acl bootstrap` command and no `BootstrapAcl` RPC. An
endpoint that works "whenever no management token exists" can be won by any
workload that reaches the manager after a first start or a recovery, so the
initial token is only ever minted by the manager process itself.
