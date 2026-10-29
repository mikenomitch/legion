# Apps

An **app** is one of Legion's add-ons: the autoscaler, and whatever you
install from a manifest. It is three things:

1. a **job** in the reserved `legion-system` namespace;
2. a **grant**: what that job's `legion` identity may do in Legion
   ([Workload Identity: Permissions](workload-identity.md#permissions));
3. **registrations**: the UI it adds (pages, panels, badges, and actions,
   drawn from the app's declarative views, and sandboxed pages for what the
   views cannot draw; see [Views and actions](#views-and-actions) and
   [Sandboxed pages](#sandboxed-pages)) and the jobspec keys it adds
   ([Jobspec extensions](#jobspec-extensions)).

A **manifest** declares all three. An **app record**, stored on the
managers, holds the installed manifest, the operator's settings and config,
and the grant. The manager's **app reconciler** keeps the job matching the
record: it submits the job, re-renders it when the record changes, restarts
it when it stops or fails, and is the only writer of it: direct edits to an
installed app's job are refused.

An app is an ordinary job. It is scheduled, restarted, rolled out, logged,
metered, and exec'd into (by those allowed) like any other. Everything that
makes it an app is around the job.

```console
$ legion app list
NAME        VERSION  SOURCE   STATUS   PLACEMENT  REPLICAS  UI   DETAIL
autoscaler  0.1.0    bundled  running  managers   1/1       -    on legion-system-x8f2k1qz

$ legion app status autoscaler
$ legion app logs autoscaler -f
$ legion app config get autoscaler
$ legion app set autoscaler --placement any
$ legion app disable autoscaler
$ legion app install ./gitops.ncl
```

## Bundled and installed apps

**Bundled apps** are compiled into the `legion` binary. Today there is one,
`autoscaler`. The first manager to lead a new cluster's control plane
**seeds** a record for each bundled app, enabled, with its manifest's
permissions granted by `legion:bootstrap`, and the app starts with nothing
configured. A bundled app runs the node's own binary: its job's command is
`legion:self app run <name>` (see [Inside an app](#inside-an-app)), so it is
always the same build as the node that runs it, with nothing to download.

Bundled apps can be disabled, configured, and moved, but not uninstalled.
They upgrade with the binary ([Upgrades](#upgrades-of-bundled-apps)).

The **autoscaler** acts on every task with `autoscale` in its jobspec
([autoscaling.md](autoscaling.md)): it watches the effective policies, reads
the cluster-wide scaling inputs once per interval, and writes counts through
`ScaleTask`, reporting each evaluation back to the manager. It also runs
[node groups](autoscaling.md#node-groups): it asks a provider for nodes,
mints their join tokens, and drains, terminates, and forgets the nodes it
scales in. Its grant is `scale:read` and `scale:write` in every namespace,
`job:read` (to read what a drain would move), `node:read` and
`node:write` (node groups), and `kv` read and write under its reserved
`legion/apps/autoscaler/` (its replicas' shard leases): it can resize,
drain, and forget nodes, not redeploy. `legion app set autoscaler
--replicas 3` splits the policies between three replicas
([autoscaling.md, "Replicas"](autoscaling.md#replicas)). Its config takes `interval` (default `"15s"`), `mode`
(`"dry-run"` forces every policy into dry-run), `plugins`, the metric
sources, strategies, and targets the operator installs for policies to
name ([autoscaling.md, "Plugins"](autoscaling.md#plugins)), and
`identities`, extra workload identities for its task (a cloud provider's
web identity token, so a node target needs no stored keys). A plugin's
`source`, `command`, and `url`, an `exec` target's `config.command`, and
`identities` are code fields: changing one changes the code the app runs
(or the credentials it holds), so it needs the app's whole grant. A built-in source's `config` (its address) and `namespaces`
need only `app:write`. When it stops reporting, `legion scaling status`
says `NOT RUNNING`. Its UI is built from app views: an Autoscaling page in
the sidebar, a panel on every autoscaled job's page, a badge on each
autoscaled task, and a node-group panel on node pages
([autoscaling.md, "In the UI"](autoscaling.md#in-the-ui)).

**Installed apps** come from a manifest file or URL (`legion app install`).
They carry their own `version`, run their own code (an artifact or an image,
like any job), and can be upgraded and uninstalled.

### Seeding and the manager config

A manager config may set seed-time settings for bundled apps:

```nickel
# configs/manager.ncl
apps = {
  autoscaler = { enabled = false },   # also: placement, replicas, config
},
```

The block is read **only when a bundled app's record is first seeded**.
Managers can disagree about their config, and the record is the single
source of truth afterwards, so later edits do nothing; a manager that finds
the block for an app that already exists logs that it was not applied. Use
`legion app` from then on. A `config` that does not match the app's schema,
or a bad `placement` or `replicas`, is logged and replaced with the
manifest's default.

## The manifest

A manifest is Nickel or JSON. Unknown fields are errors.

```nickel
{
  name = "gitops",                       # the app, and its job: legion-system/gitops
  description = "Deploys jobs from git repositories.",
  version = "0.2.0",                     # required for installed apps
  placement = "managers",                # managers (default) | workers | any
  replicas = 1,                          # default 1, at most 32
  job = {                                # a jobspec, minus name and namespace
    task = {
      name = "sync",
      runtime = "exec",
      artifacts = ["https://example.com/gitops/0.2.0/legion-gitops-linux-amd64.tar.gz"],
      config = { command = ["./legion-gitops"] },
      resources = { cpu = 200, memory = 128 },
    },
  },
  permissions = [                        # the grant the app asks for
    { resources = ["job"], actions = ["read", "write"], namespaces = ["team-*"] },
    { resources = ["template"], actions = ["read"] },
    { resources = ["kv"], actions = ["read"], paths = ["legion/apps/gitops/*"] },
  ],
  ui = { ... },                          # pages, panels, badges, actions, sandboxed pages (Views and actions)
  extensions = [ ... ],                  # jobspec keys (Jobspec extensions)
  config = {
    schema = { type = "object", properties = { repos = { type = "array" } } },
    default = { repos = [] },
  },
  codeFields = [                         # config that changes the code the app runs
    "/plugins/*/url",
    { pointer = "/plugins/*/source", artifact = true },
  ],
}
```

[`configs/apps/example-app.ncl`](../configs/apps/example-app.ncl) is a
complete example with comments.

| Field | Meaning |
| --- | --- |
| `name` | Lowercase letters, digits and `-`, starting with a letter, at most 40. The job is `legion-system/<name>`. |
| `description` | One line. |
| `version` | Required for an installed app, any non-blank string without spaces; a bundled manifest has none. |
| `placement` | Where the app runs by default ([Placement](#placement)). |
| `replicas` | Each task's `count` by default. |
| `job` | A jobspec without `name` or `namespace`, which Legion sets. Must define `task` or `tasks`, and parse as a jobspec. An `exec` task may not set `user`, and nothing in it may be host-level besides the `exec` runtime itself ([Trust model](#trust-model)). |
| `permissions` | ACL rules in the shape `legion acl policy apply` takes: what the app's `legion` identity asks to be granted. |
| `ui` | UI registrations: the slots the app's views appear in, the actions they offer, the port they are served on, and the refresh interval ([Views and actions](#views-and-actions)). Unknown fields are errors. |
| `extensions` | The keys the app adds to the jobspec, one per extension point, each with a JSON Schema ([Jobspec extensions](#jobspec-extensions)). |
| `config.schema` | A JSON Schema (draft 7) the config must match. It must describe an object, be at most 64 KiB, and use only local `$ref`s (`"#/…"`); a remote ref is refused before anything is fetched. Default: any object. |
| `config.default` | The config a fresh install gets. Must match the schema. Default `{}`. |
| `codeFields` | JSON pointers into the config whose values change the code the app runs. A `*` segment matches every key of an object or element of an array. An entry `{ pointer, artifact = true }` is also fetched as an artifact of the app's job (below). |
| `namespace` | Reserved for apps owned by a tenant namespace, which Legion does not support yet: setting it is an error. |

### What Legion renders

The reconciler renders the app's job from the record, as a pure function of
it (and, for a bundled app, the build marker):

- the manifest's `job`, named `legion-system/<app>`;
- every task's `count` is the app's `replicas`;
- the placement becomes each task's `permit`, constraints, and affinities
  (below), replacing the manifest's permit and adding to its constraints;
- `resources` from `legion app set --cpu/--memory` override each task's;
- every `exec` task runs as the **app's own uid** (and a gid of the same
  number): one per app, the lowest free one from `1900000000` up, assigned
  when the app is installed or seeded and kept on its record (`legion app
  status` shows it, and `GetApp` returns it as `uid`). No two apps share
  one, so one app cannot read another's `secrets/` (its `legion` token) or
  config file. The uid needs no passwd entry on the node (its
  `HOME` is `/nonexistent`); keep `1900000000`-`1900065535` free of other
  users. A manifest may not name an exec task's `user`;
- every task's `legion` identity carries the app's recorded grant, as its
  `permissions` (a `policies` list in the manifest is dropped);
- the config is written as a file into each `exec` and `container` task's
  directory, named by `$LEGION_APP_CONFIG`;
- each string value at an artifact code field becomes an artifact fetched into
  `code/<path>/` in the task directory, with the sibling `checksum` string,
  if there is one, as its checksum (`/plugins/aws/source` lands in
  `code/plugins/aws/`);
- a bundled app gets the build-marker constraint, and `LEGION_APP_BUILD` in
  its environment.

### Placement

Placement is a setting, not part of the build: `legion app set autoscaler
--placement workers` changes the record, and the reconciler resubmits the
job (a normal rollout).

**Placement takes `operator:write`.** Where an app runs is where its
`legion` token lands and where its code runs with its grant, so changing
`--placement` or `--pool`, or installing a manifest whose `placement` is not
`managers`, needs `operator:write` as well as `app:write`. With `app:write`
alone, a person could move an app with a broad grant onto a pool where they
have root, and read its token there.

```text
Error: changing the placement or pool decides which nodes hold app autoscaler's token and run its code with its grant, so it needs operator:write as well as app:write
```

| `placement` | Permit | Constraint / affinity |
| --- | --- | --- |
| `managers` (default) | pools `legion-system` and `default` | constraint `legion.role = "manager"` |
| `workers` | pool `default` (or `--pool`) | constraint `legion.role is_not_set` |
| `any` | both (`legion-system` and `default`, or `--pool`) | affinity toward `legion.role = "manager"` |

`legion.role = "manager"` marks a manager's own worker: its
[system worker](architecture-managers-workers.md#the-system-worker), or a
`--run-worker` manager's worker. Keying `managers` on the attribute rather
than the pool is what makes it work on both. With `systemWorker.enabled =
false` on every manager, `managers` placement has nowhere to run, and
`legion app status` says so; set `--placement any` or `workers`.

## Lifecycle

### Installing

```console
$ legion app install https://example.com/gitops/0.2.0/app.ncl
gitops 0.2.0 runs its own code (1 artifact) on your managers, as its own unprivileged user,
in legion-system, and asks for:
  job       read, write   namespaces: team-*
  template  read
  kv        read          paths: legion/apps/gitops/*
UI: a GitOps page, a Sync panel on job pages, one action (Sync now, needs job:write)
Your token holds all of these. Grant them and install? [y/N] y
installed gitops 0.2.0; job legion-system/gitops starting
```

`install` takes a file (`.ncl` or `.json`) or an `http(s)` URL. A URL is
fetched by the CLI (at most 1 MiB); a Nickel manifest from a URL must be
self-contained, because imports are refused. The CLI asks the manager for a
dry run first (`InstallApp` with `dry_run`), prints the prompt, and asks
before installing; `--yes` skips the question (and is required with
`--json`), `--dry-run` stops after the prompt, `--config <file>` sets
the initial config, and `--key <point>.<key>=<new>` installs a jobspec key
under another name ([Keys cannot collide](#keys-cannot-collide)). An app
that declares jobspec keys adds what installing does to stored values to the
prompt: the pending values it adopts, and how they fare against its schemas.

**You can grant only what you hold.** Installing needs `app:write` on the
app's name, and every permission the manifest asks for, checked exactly as a
job submit checks a job's `permissions`
([Job grants](auth-and-acls.md#job-grants)). The grant is recorded on the app
record in the installer's name:

```text
Error: app gitops 0.2.0 asks for job:write in team-*, which your token does not hold
Hint: have someone who holds it do this, or ask for it to be granted to you
```

**A page's `rpcs` count too.** Every permission behind an RPC a sandboxed
page lists is checked the same way, everywhere the RPC reaches, reserved
scopes included ([Which RPCs a page may call](#which-rpcs-a-page-may-call)):
a page runs its RPCs as whoever views it, so installing it takes holding
what it can make them do. A placement other than `managers` takes
`operator:write` ([Placement](#placement)).

**Nothing host-level.** Install renders the app's job and refuses one that
would make the worker act for it on the node: an `exec` health check, a
`file://` (or local `git::file://`) artifact or image, an artifact with
`auth.nodeIdentity`, or an `exec` task's file at an absolute host path, its
own `config.cgroup`, or a negative OOM score
([Namespaces](namespaces.md#reserved-namespaces) has the list). On a manager the worker is root, so any of them would
be root on the managers. Apps don't need them: health checks are `http` or
`tcp`, and code comes from a URL. The refusal holds for every installer,
management tokens included; a job that needs one is an ordinary job.

```text
Error: app checker: task "main" declares exec health check "probe", which the worker runs on the host, and tasks in namespace legion-system may not: they run on the managers' system workers, where the worker that would do it is root.
Hint: use an `http` or `tcp` health check, and fetch artifacts from a URL
```

**The job must pass admission.** Install then runs the rendered job through
everything the reconciler's submit will check, as the reconciler submits it
(the app's recorded grant, the namespace's policies and quota, `kv:read` on
every KV key the job references, artifacts and their pins, volumes,
capabilities), and writes nothing. A job the reconciler would be refused is
refused at install, with the reason, instead of installing and sitting in
`reconcile refused`:

```text
Error: app rooted's job would be refused when the reconciler submits it: permission denied: ... (the jobspec reads kv secret/db; the submitter needs kv read on it)
```

Upgrades, config changes, and settings changes run the same check. A pin
whose source cannot be reached right now is `Unavailable`: retry.

**State an earlier app left.** An app's own KV lives under
`legion/apps/<name>/`, and `legion app uninstall` leaves it unless
`--purge` ([Uninstalling](#uninstalling)). When keys are there, the prompt
says so before you approve, because the new app's grant may reach them:

```text
1 KV key under legion/apps/gitops/ was left by an earlier install of an app named gitops; this app takes it over (`legion app uninstall --purge` removes an app's keys).
```

A bundled app's name cannot be installed, and an installed name cannot be
installed again (`legion app upgrade` replaces a manifest). Install never
takes over a job: if `legion-system/<name>` exists and its latest version is
not stamped `managedBy` this app (an operator's own job, say), install is
refused (`FailedPrecondition`); destroy the job or pick another name. The
stopped job an uninstall leaves is still the app's, so reinstalling works.

### Upgrading

```console
$ legion app upgrade gitops ./gitops-0.3.0.ncl
gitops 0.3.0 runs its own code (1 artifact) on your managers, as its own unprivileged user, ...
Permission changes:
  + net       read
  - template  read
Approve the new permissions and upgrade? [y/N] y
upgraded gitops 0.2.0 → 0.3.0
```

A new manifest is new code running with the grant, so upgrading needs
`app:write` and every permission of the **new** manifest, and of its pages'
`rpcs`, as installing does; its job is held to the same rules (nothing
host-level, no `user`). When the new manifest asks for anything the installed one did not, the
upgrade needs approval (`--yes`, or the question); one that only keeps or
drops permissions upgrades without asking. A token that does not hold the
new manifest's permissions is refused before anything is asked. The stored
config must match the new schema; `--config <file>` replaces it in the same
step. The new job passes admission first, as at install. `--dry-run` shows
the diff and changes nothing; with `--json` it carries `uncovered` (the
permission the token lacks, empty when it holds them all) and
`orphanedJobs`. Bundled apps cannot be upgraded this way. The prompt reports
every stored jobspec value of the app's keys checked against the new
schemas ([Installing, upgrading,
uninstalling](#installing-upgrading-uninstalling)); a new manifest that
drops an extension point jobs set a key at needs `--force`.

**Your settings stay.** An upgrade keeps the replicas, placement, pool, and
resources you set (or seeded), even when the new manifest's defaults differ:
they are the operator's, and the manifest's are only where an install
starts. The prompt says so when they differ:

```text
Keeps replicas 1 (this version's default is 3; `legion app set gitops --replicas 3` takes it).
```

**An upgrade that does not take.** `upgraded gitops 0.2.0 → 0.3.0` means the
record holds the new manifest. Whether the new version runs is the app's
status ([Status](#status)): `upgrading` while the previous version still
serves, `degraded: upgrade failing: …` when the new version's rollout failed
or rolled back (the old version keeps running), never `running` on the old
version. To retry after fixing the cause (a registry, a quota), run `legion
app enable <app>` or `legion app upgrade` with the same manifest: either is
a write, and the reconciler resubmits a job that does not run healthy at the
version the record renders, with a forced reschedule. There is no `legion
app rollback`; to go back, upgrade with the previous manifest.

### Config

```console
$ legion app config schema autoscaler     # JSON Schema
$ legion app config get autoscaler        # JSON, valid input for `config set`
$ legion app config set autoscaler -f autoscaler.ncl
autoscaler: config changed; its job rolls with the new config
```

The config lives on the app record. `config set` replaces it whole,
validated against the manifest's schema, and the reconciler resubmits the
job with the config rendered as a file. There is no live reload: a config
change is a rollout, so the job's version history is the config's history.

**Code fields need the grant.** A change that adds, removes, or changes a
value at one of the manifest's `codeFields` (for the autoscaler, a plugin's
`source` or `url`) changes what code runs with the app's grant. It needs the
caller to hold every permission of the app's recorded grant, as installing
does; other config changes need only `app:write`. Without this, `app:write`
on the autoscaler would be a path to `scale:write` in every namespace.

```text
Error: changing app autoscaler's code (/plugins/aws/source) runs code with its grant, which asks for
scale:write in *, which your token does not hold
```

The job a new config renders is held to the install rules too: an artifact
code field set to a `file://` source is refused (`InvalidArgument`) for
everyone, grant or not.

### Settings, enable, disable

```console
$ legion app set autoscaler --replicas 2 --placement any
autoscaler: settings changed (replicas 1 → 2, placement managers → any)
$ legion app set autoscaler --cpu 500 --memory 512    # per task; 0 clears an override
$ legion app disable autoscaler     # stops the job; keeps the record, config, and grant
$ legion app enable autoscaler
```

`--pool` picks the worker pool for `workers` and `any` placement. Changing
`--placement` or `--pool` needs `operator:write` as well as `app:write`
([Placement](#placement)); replicas and resources need only `app:write`.
Enabling an app that is already enabled is not a no-op: it is a record
write, which retries a reconcile a refusal parked ([Failures](#failures)),
and resubmits a job that does not run healthy at its current version (a
failed upgrade). `set`, `config set`, `enable`, `disable`, and `uninstall`
take `--dry-run`: they validate (a settings or config change runs the job's
admission too) and say what would change, without changing it.

### Uninstalling

```console
$ legion app uninstall gitops
uninstalled gitops; job legion-system/gitops stopped and 3 grant(s) revoked (`legion job destroy legion-system/gitops` removes the job)
```

Uninstall disables the app, stops its job, revokes the job's grants (its
workload tokens lose them at their next request), and deletes the record.
Disabling first means a reconciler submit already in flight loses its
compare-and-set on the record ([How an app stays
running](#how-an-app-stays-running)). The stopped job stays until someone
destroys it; with the app gone it is an ordinary job (`legion job destroy`
works). Bundled apps cannot be uninstalled; disable them. A retry with the
same `--idempotency-key` gets the first reply back, whole.

**The app's KV stays unless `--purge`.** Keys under `legion/apps/<name>/`
(the app's own state, which its grant usually covers) are left, and the
reply says how many. `--purge` deletes them, and their locks, in the same
replicated entry as the record; it needs `kv:write` on that prefix besides
`app:write`. A later app installed under the same name, from any vendor, is
told about leftover keys in its prompt, since its grant may reach them.

```console
$ legion app uninstall gitops --purge
uninstalled gitops; job legion-system/gitops stopped and 3 grant(s) revoked (`legion job destroy legion-system/gitops` removes the job); deleted 4 KV key(s) under legion/apps/gitops/
```

**A record that does not decode** (`legion app list` shows it `corrupt`) is
removed by name the same way, a bundled app's included; the reconciler seeds
a bundled app again from the running build within a minute ([Upgrading
Legion](#upgrading-legion)).

While jobs set the app's jobspec keys, uninstall needs `--force`, and the
values stay stored, flagged `orphaned`:

```text
Error: 2 jobs set backup's jobspec keys: orders-db, cache. Uninstalling leaves their values stored, flagged orphaned, and each job's next submit fails until the key is removed (or --allow-unknown-fields is passed)
Hint: pass --force to uninstall anyway
```

## How an app stays running

The reconciler runs on the manager that leads the control plane's Control
lane. It handles one app at a time, with at most one submit in flight per
app. Each pass, for each app:

1. **Render** the job from the record (above), and digest it.
2. **Compare the digest**, not the job. Every version the reconciler submits
   is stamped `managedBy = { app, digest }`. When the job's latest version
   carries the fresh render's digest, there is nothing to do. The reconciler
   never diffs the rendered job against the stored one: the stored config has
   defaults filled in and fields normalized, so a structural diff would find
   differences that are not there.
3. **Check the job's state.** An enabled app whose job was stopped is
   resubmitted; a disabled app's running job is stopped.
4. **Submit** through the normal submit path, as `legion:app-reconciler`.
   The submit is checked against the grant recorded on the app, not against
   any principal's own rights, and the version's job grant is recorded as
   `<granter> (app <name>)`.

**Which writes keep the stamp.** The reconciler's own submits set it. A KV
re-render of the job ([KV Store](kv-store.md)) keeps it, because it
re-renders the same spec. No other write may store a version of an
installed app's job (below); a version stored some other way (one a release
before this one let through) has no stamp, and the next pass reverts it,
naming who stored it.

### Editing an app's job

While an app is installed, its job is the reconciler's alone. Every direct
write to `legion-system/<app>` is refused (`FailedPrecondition`), for every
caller, management tokens included, and the error names the `legion app`
verb that does it:

```text
$ legion job scale legion-system/autoscaler --count 0
Error: job legion-system/autoscaler is app autoscaler's, and the app reconciler owns it, so scaling it directly is refused (docs/apps.md, "Editing an app's job")
Hint: `legion app set autoscaler` changes its replicas, placement and resources, `legion app config set autoscaler` its config, `legion app disable autoscaler` stops it, and `legion app uninstall autoscaler` removes the app, after which the job is an ordinary job
```

| Write | Refused | Instead |
| --- | --- | --- |
| `RunJob`, `RunJobTemplate` (and `PlanJob`, which predicts it) | yes | `legion app set`, `legion app config set`, `legion app upgrade` |
| `ScaleTask` (`legion job scale`) | yes | `legion app set --replicas` |
| `StopJob` | yes | `legion app disable` |
| `DestroyJob` | yes | `legion app uninstall`, then `legion job destroy` |
| `StopRollout` | yes | `legion app upgrade` with the previous manifest |
| `AdvanceRollout` | no: it moves toward the version the reconciler submitted | |

Refusing beats reverting: a GitOps tool or a CI job that syncs
`legion-system` gets an error it can act on, instead of fighting the
reconciler forever (each round a new version and two restarts), and a scale
override, which is not a version and so survives every resubmit, can no
longer quietly disable an app. The `job info` banner says so:

```console
$ legion job info legion-system/autoscaler
...
managed by app autoscaler (digest 3f9a12c4…); direct edits are refused. Use `legion app`.
```

**Exactly once per transition.** Each submit carries the idempotency key
`app:<name>:<digest>:<from>`, where `<from>` is the state it starts from (the
job's latest version, `none`, `stopped-<version>`, `failed-<version>`, or,
for a retry after a record write, `<version>@<updated_at>`). If the leader dies
after its submit lands but before it records that, the next leader's pass
finds the stamp and does nothing, and a retry of the same key is
deduplicated. A new transition (restarting a job that failed again,
enabling within a day of disabling) starts from another state and gets a new
key.

**Compare-and-set on the record.** A submit can take seconds (pin
resolution reaches registries), and the record can change meanwhile. The
submit carries the record's `updated_at` it rendered from, and the store
refuses it, in the same entry as the version, unless the record is still
installed, enabled, and unchanged. So an uninstall or a disable that lands
mid-submit never has the job come back with a fresh grant; the next pass
renders from the new record. And a job still stamped for an app that has no
record (stopped by nothing) is stopped, and its grants revoked, by the next
full pass.

**A failed job is restarted.** A job whose own policy gave up (a reschedule
policy's `moveCount`, a KV key's `onDelete = "fail"`, a gang that lost its
quorum) has status `failed`. The reconciler resubmits it (the same version
starts again), backing off from 5 seconds doubling to 5 minutes while it
keeps failing; the app reads `degraded: its job failed; restarting it in
20s (restart 3)`. The backoff resets once the job runs healthy.

**Triggers.** A pass runs for an app when its record changes (any `legion
app` write), when its job changes (a new version, a stop), when a node joins
or re-registers (bundled apps' build markers), when a manager takes over the
Control lane (it also seeds bundled apps then, and on every full pass), and
every 60 seconds for every app, in case an event was missed. Bursts
coalesce: changes that arrive during a pass queue one more pass, not one
each. A manager that takes over the Control lane starts with no memory of
failures, backoffs, or parked refusals, even ones from its own earlier
term: it tries everything once.

### Failures

- A failed submit retries with backoff, from 5 seconds doubling to 5
  minutes. The app's status is `degraded` with `reconcile failing: <error>`,
  and `app.reconcile_failed` is emitted on the first failure and then at
  most every 10 minutes. A write to the app retries at once.
- A submit the manager **refuses** (the job fails validation, a grant check,
  or a quota) does not retry on its own, since retrying cannot fix it. The
  status reads `reconcile refused: <error>`, and the reconciler waits for the
  next change to the app's record or to its build marker. `legion app enable
  <app>` is a write that retries it after you fix the cause elsewhere (a
  namespace quota, say). A job that already runs the render is never held
  back by a refusal parked earlier.
- **A source that cannot be reached is not a refusal.** A pin whose
  registry or git remote refuses the connection, times out, or answers 5xx
  or 429 fails the submit with `Unavailable`, which backs off and retries
  like any other failure; a reference that does not resolve (a tag that
  does not exist, refused credentials) is still a refusal.

### Status

`legion app list`, `legion app status`, and `GetApp` derive an app's status
when they are read:

| Status | When |
| --- | --- |
| `stopped` | the app is disabled |
| `degraded` | the last reconcile failed; its job failed (and is being restarted); a bundled upgrade is held (`upgrade held: …`); the latest version's rollout failed or rolled back (`upgrade failing: …`); or fewer allocations of the latest version run healthy than it asks for (`2/3 running`) |
| `upgrading` | the job's latest version is not fully healthy yet, and the previous version still serves |
| `pending` | nothing runs yet: no build marker, no job yet, the job was stopped and is being restarted, or the scheduler's reason it is blocked (`no room in pool legion-system`) |
| `running` | every allocation it asks for runs **the job's latest version** and reports healthy (its health checks pass) |
| `corrupt` | this manager cannot decode the app's record (the detail says why); nothing reconciles it until `legion app uninstall` removes it |

Only allocations of the job's latest version count as running, so an
upgrade whose new version never became healthy cannot read `running` while
the old version serves.

`legion app status` also shows the record, the grant and who approved it,
the reconciler's last action, error, and digest, the build marker and what
the app waits for, and notes (no system worker for `managers` placement,
workload identity off).

## Upgrades of bundled apps

A bundled app's code is the binary of the node it runs on. The reconciler
restarts it only when there is one new build to move to:

- Workers report `buildVersion` (the package version) and `buildId` (a hash
  of their own executable, computed at startup) when they register. A
  rebuild without a version bump changes `buildId`.
- The app's **build marker** is the (`buildVersion`, `buildId`) pair, defined
  when every ready node its placement allows reports the same
  `buildVersion`. When they all run one binary, its `buildId` is the
  marker's; when they run different binaries of that version (amd64 and
  arm64 nodes, a distro package beside a tarball), the marker's build id is
  `mixed-` and a digest of the set, so a mixed fleet agrees, and a rebuild of
  any one binary still rolls the app. Nodes that report `unknown`, an
  unparsable version, or no build id are left out.
- While those nodes disagree (during an upgrade), the marker keeps its last
  value: a straggler holds it, and an older node joining later does not move
  it backwards. When they agree again, the marker moves and the app rolls
  once. That covers a deliberate downgrade of the whole set too.
- With no ready eligible node there is no marker yet, and the app waits
  (`pending: waiting for an eligible node to report its build`).
- The job carries `{ key = "buildVersion", operator = "version_matches", value
  = ">=<marker version>" }`, so a reschedule never lands it on an older node.

```console
$ legion app list
NAME        VERSION  SOURCE   STATUS   PLACEMENT  REPLICAS  UI  DETAIL
autoscaler  0.14.0   bundled  running  managers   1/1       -   upgrade waiting: 2 of 3 eligible nodes on 0.15.0 (waiting for mgr-3)
```

**New bundled manifests.** A new build can change a bundled app's job,
permissions, or config schema. The reconciler adopts the new manifest only
when the leading manager itself runs the marker's version, since it can
read only manifests compiled into its own binary (builds of one version for
other architectures carry the same manifest); until then it renders from
the manifest stored on the record. New permissions are recorded as granted
by `legion:bootstrap` and reported in the `app.upgraded` event.

**A config the new build would refuse holds the upgrade.** When the stored
config does not match the new manifest's config schema (a property renamed
or removed under `additionalProperties = false`), the new build's app would
refuse it and crash-loop. So the reconciler does not adopt the manifest:
the job stays rendered for the previous marker, the app reads `degraded`,
and `app.reconcile_failed` says why, once:

```text
autoscaler  0.14.0  bundled  degraded  managers  1/1  -  upgrade held: build 0.15.0+3f9a12c4e1b0 changes autoscaler's config schema, and the stored config does not match it (...); the app stays on its previous build until `legion app config set autoscaler <file>` sets a config that does
```

`legion app config set` accepts a config that matches the new build's
schema while the upgrade is held, even where the stored schema refuses it;
the next pass adopts the manifest and rolls the app. The job keeps the
previous build marker, but `legion:self` runs whatever binary its node has:
an allocation that restarts on an upgraded node runs the new build with the
old config, and fails until the config is set.

Limits:

- **"Once" counts the reconciler's own restarts.** A manager restarting for
  its upgrade still kills the allocations on its system worker, and the
  scheduler places them again, possibly on a manager that has not upgraded.
- **Pre-releases.** `version_matches` uses semver requirements, under which a
  pre-release of a newer version (`0.16.0-rc.1`) does not satisfy
  `>=0.15.0`. During a mixed window, such nodes cannot host the app; once
  every eligible node runs the pre-release, the marker moves to it.

## Upgrading Legion

Legion breaks freely between releases ([upgrades](upgrades.md)), and app
records are stored data. So the store reads them leniently and installs
read them strictly:

- **A field this release does not know** in a stored manifest or grant (one
  another release added, or one it renamed or removed) is dropped when the
  record is read. A typo in a manifest you install is still an error.
- **A record that still does not decode** (a field whose type changed) is
  that record's problem only. It is logged once per manager, counted as
  `legion_apps{status="corrupt"}`, listed by `legion app list` with status
  `corrupt` and the reason, and skipped by the reconciler and by the
  registry of jobspec keys, so every job submit and every other app keeps
  working. `legion app uninstall <app>` removes it by name; a bundled app is
  then seeded again from the running build within a minute.

## Trust model

- **An app's power is its grant**, and the grant is never more than a person
  held: the installer (checked at install), an upgrader (checked at
  upgrade), or whoever changes a code field (checked at `config set`).
- **A button press is the viewer's, not the app's.** An app that acts on an
  action through its viewer client ([Acting as the
  viewer](#acting-as-the-viewer)) does so as `<viewer> via app <app>`,
  holding only the action's declared permission on the context it was
  pressed on, within its grant; history records the person. The app's code
  can still call with its own grant instead, so the manifest's declared
  permission is the bound of a well-behaved app, and the grant the bound of
  any.
  Bundled apps are the exception: their grants are recorded by
  `legion:bootstrap` at seed time, without a check, because the manifest is
  part of the binary you chose to run, which is already the manager.
- **`app:write` is not the grant.** It changes replicas, resources, config
  outside code fields, and enabled state; it cannot add code to an app, and
  it cannot move one (placement takes `operator:write`,
  [Placement](#placement)).
- **Apps run unprivileged, each as its own user.** Tasks in `legion-system`
  may not run as root, and privileged containers are refused there
  ([Namespaces](namespaces.md#reserved-namespaces)). Every app's `exec`
  tasks run as the app's own uid, so one app cannot read another's
  `secrets/` and borrow its grant. Root on a manager could read the raft
  state and the data directory, so an app's blast radius stays its grant.
- **Nothing host-level in an app's job.** An `exec` health check, a
  `file://` artifact, `auth.nodeIdentity`, or a file written at a host path
  would have the worker, root on a manager, act for the app; they are refused in `legion-system` whoever
  submits them, and install, upgrade, and `config set` refuse a render that
  holds one. The reconciler holds no `host` grant: an app's own `exec` task,
  which runs as its uid, is the one host-level construct it may submit.
- **An app is its stamped job.** Only an allocation of a `legion-system/<app>`
  version the reconciler stamped `managedBy` that app speaks for the app
  (app-plane RPCs, delegations) or receives its viewer assertions; a version
  someone submits straight to the job does not.
- **Tenants cannot reach apps.** ACL globs do not match the reserved
  `legion-*` namespaces or the `legion/` KV prefix unless the pattern names
  them, so a tenant admin with `namespaces = ["*"]` cannot edit, exec into,
  or borrow the identity of an app's job.
- **Grant changes are visible.** `legion acl grants list --job
  legion-system/<app>` shows every version's grant and its granter, and the
  app events name who installed, upgraded, changed, enabled, or disabled an
  app.
- **App code never runs in the UI's origin.** Views are data the SPA draws.
  A sandboxed page runs in an opaque-origin iframe that holds no token and
  reaches Legion only through the bridge, as its viewer, within its `rpcs`,
  with every write confirmed. What a page *can* do is read what its viewer
  can read through those RPCs and send it elsewhere
  ([What the sandbox does not stop](#what-the-sandbox-does-not-stop)). That
  reach is bounded by its installer: listing an RPC takes holding its
  permissions everywhere, so a page never makes a viewer do what whoever
  installed it could not, and no page may change ACLs, tokens, grants, or
  apps.

## ACLs

`app` is a cluster-scoped [ACL resource](auth-and-acls.md#resources-and-actions)
with a `names` selector, matched against app names:

```nickel
{ resources = ["app"], actions = ["read"] },                                   # see every app
{ resources = ["app"], actions = ["read", "write"], names = ["autoscaler"] },  # manage the autoscaler
```

| RPC | Needs |
| --- | --- |
| `ListApps` | `app:read` (the list is filtered by `names`) |
| `GetApp` | `app:read` on the app |
| `InstallApp` | `app:write` on the app, every permission the manifest asks for, every permission behind its pages' `rpcs`, and `operator:write` for a placement other than `managers` |
| `UpgradeApp` | `app:write`, every permission the new manifest asks for, and every permission behind its pages' `rpcs` |
| `UninstallApp`, `SetAppEnabled` | `app:write` |
| `UpdateAppSettings` | `app:write`, and `operator:write` to change placement or pool |
| `SetAppConfig` | `app:write`, and the app's whole grant when a code field changes |
| `ListUiExtensions` | `app:read` (filtered by `names`, and by each entry's permission) |
| `GetAppView` | `app:read` on the app, and the entry's permission on the context |
| `InvokeAppAction` | `app:read` on the app, and the action's permission on the context |
| `GetAppPage` | `app:read` on the app, and the page's permission on any scope |
| `GetJobSchema` | `job:read` on any scope (the schema describes jobspecs, not jobs) |
| `WatchAppExtensions` | app-plane: the app's own workload identity (its values only), or a management token naming the app |
| `RunJob`, `RunJobTemplate` setting a key with `requires` | the key's permission on the job, for a new or changed value ([At submit](#at-submit)) |

Every mutating RPC takes `idempotency_key`, and a repeat returns the first
reply with `deduplicated` set (`--idempotency-key` on the CLI).

## Views and actions

Apps appear in the UI through **declarative views**: an app answers a
request for a view with a small JSON document (`legion-view/1`) built from a
closed set of blocks, and the SPA draws it with Legion's own components. A
view carries no code: an app's own code reaches the browser only as a
[sandboxed page](#sandboxed-pages), outside the SPA's origin. The same views
print in the terminal
(`legion app view`) and reach agents (`legion_app_view`), so whatever a
person sees on a panel, an agent can read.

```console
$ legion app view gitops job web
Sync

  Commit: 3f9a1c2   Synced: 2m ago   Drift: none (ok)

Syncs (1h)
  applied      ▁▁▁▁▄▄▄▄█████                            now 6, min 4, max 6

  2026-10-28 12:02:25Z  Applied 3f9a1c2
                        team-a/web from git@example.com:team-a/deploy.git
Actions: Sync now

  sync-now (Sync now): needs job:write; you may run it from the UI
```

### Slots

An app declares where its views appear in its manifest's `ui` block:

```nickel
ui = {
  port = "api",                                          # the task port views are served on (default "api")
  nav  = [{ title = "GitOps", icon = "gitBranch", view = "repos" }],
  overview = [{ title = "GitOps sync", view = "summary" }],
  job  = [{ title = "Sync", view = "job-sync",
            permission = { resource = "job", action = "read" } }],
  task = [{ badge = "task-source", permission = { resource = "job", action = "read" } }],
  node = [{ title = "Deployed here", view = "node", permission = { resource = "node", action = "read" } }],
  actions = [
    { name = "sync-now", title = "Sync now", confirm = "Sync {job} from its repository now?",
      inputs = { mode = { type = "enum", values = ["apply", "dry-run"], default = "apply" } },
      permission = { resource = "job", action = "write" } },
  ],
  refresh = "10s",
}
```

| Slot | Where | Context the view gets |
| --- | --- | --- |
| `nav` | a page in the sidebar's **Apps** section (`/apps/<app>/<view>`) | none |
| `overview` | a card on the cluster overview | none |
| `job` | a panel on a job's page | `namespace`, `job` |
| `task` | a badge on a task's row on the job page (`badge`), and a panel on the task's page (`view`) | `namespace`, `job`, `task`, `group` |
| `node` | a panel on a node's page | `node` |

| Entry field | Meaning |
| --- | --- |
| `title` | The page's, card's, or panel's title (the sidebar label for `nav`). Required with `view`. |
| `view` | The view the app serves for the entry: lowercase letters, digits, `-`, `_`, `.`, at most 64. |
| `badge` | `task` only: the view whose `badge` the task row shows. A task entry needs `badge`, `view`, or both. |
| `icon` | `nav` only: a sidebar icon name; an unknown one shows a default. |
| `permission` | `{ resource, action }`: who sees it ([Who sees what](#who-sees-what)). Without one, `app:read` on the app suffices. |

`ui.port` must name a port the app's job declares (`network = { ports =
[{ name = "api" }] }`); install refuses a manifest whose views have nowhere to
be served. The port is never a catalog service. `ui.refresh` is how often the
SPA re-fetches the app's views, from 5 seconds to an hour; unset, views load
with the page and on demand. At most 8 entries per slot and 32 actions.

### Who sees what

Every view and action goes through the manager, which authorizes **before
anything reaches the app**:

1. `app:read` on the app;
2. the view must be registered in the slot it is asked for (an action must be
   declared, and pressed in a `job`, `task`, or `node` slot);
3. the entry's or action's declared `permission`, checked **as the viewer**:

| Slot | A namespaced permission (`job`, `scale`, ...) is checked on | Any other (`node`, `operator`, ...) on |
| --- | --- | --- |
| `job`, `task` | the context's job: its namespace and name | the cluster (no selector) |
| `node` | every namespace (cluster-wide authority) | for `node`: the node's pool; otherwise the cluster |
| `nav`, `overview` | any scope: the viewer holds it somewhere | any scope |

A viewer without the permission gets nothing: `ListUiExtensions` with the
page's context leaves the entry out (so the panel is not drawn), and
`GetAppView` and `InvokeAppAction` answer `PermissionDenied` without
forwarding. The app sees no request at all.

On `nav` and `overview`, "holds it somewhere" is all the manager can check,
so the request tells the app where: for a namespaced permission, the viewer
assertion carries the namespace globs in which the viewer holds it
(`namespaces`, `["*"]` for a management token), and an app showing
cluster-wide rows filters by them (`Viewer::sees_namespace` in the Rust
framework).

### The view schema

A view is a JSON object:

```json
{ "view": "legion-view/1", "title": "Replicas", "blocks": [
  { "type": "stats", "items": [
    { "label": "Now", "value": "6" }, { "label": "Bounds", "value": "2–8" },
    { "label": "CPU", "value": "91%", "tone": "warn" } ] },
  { "type": "series", "title": "Replicas vs CPU (1h)", "series": [
    { "name": "replicas", "points": [[1792412400, 4], [1792412460, 6]] },
    { "name": "cpu", "axis": "right", "unit": "%", "points": [[1792412400, 91]] } ] },
  { "type": "timeline", "items": [
    { "at": 1792412545, "title": "4 → 6", "detail": "cpu 0.91 vs 0.70 (4 ready)" } ] },
  { "type": "actions", "items": [{ "action": "pause", "label": "Pause 1h" }] }
] }
```

| Field | Meaning |
| --- | --- |
| `view` | `"legion-view/1"`. |
| `title` | Overrides the entry's title. |
| `hidden` | `true`: nothing to show for this context; the slot draws nothing (`{ "hidden": true }` alone is a complete answer). |
| `badge` | `{ text, tone }`: the pill a `task` row shows (badge views). |
| `blocks` | At most 32, drawn in order. |

| Block | Fields | Limits |
| --- | --- | --- |
| `stats` | `items: [{ label, value, tone?, hint? }]`, `title?` | 1-50 items |
| `series` | `series: [{ name, points: [[unix seconds, value]], axis?: left \| right, unit? }]`, `title?` | 1-4 lines, two axes, 2,000 points a line |
| `table` | `columns: [{ key, label, type?: text \| number \| time \| duration }]`, `rows: [{ <key>: cell }]`, `empty?`, `title?` | 1-12 columns, 500 rows |
| `timeline` | `items: [{ at: unix seconds, title, detail?, tone? }]` (newest first), `title?` | 500 entries |
| `keyValue` | `items: [{ key, value, tone?, route? }]`, `title?` | 50 items |
| `notice` | `text`, `tone?`, `title?` | |
| `markdown` | `text`: CommonMark without raw HTML or images | 16 KiB |
| `actions` | `items: [{ action, label?, inputs? }]`: buttons for declared actions, `inputs` presetting some | 1-50 buttons; `job`, `task`, `node` slots only |

- **Tones** are `neutral`, `info`, `ok`, `warn`, and `danger`.
- **A table cell** is text, a number, a boolean, or `{ text, route?, href?,
  tone? }`. A `time` column shows unix seconds as a time, a `duration`
  column seconds as a duration.
- **Links** go to an in-app `route` (an absolute path such as
  `/jobs/payments/api`) or an `href` that is `http` or `https`, which opens
  in a new tab. Anything else is refused.
- **Markdown** may use headings, lists, quotes, code, emphasis, and links
  written `[text](url)`, to the same targets. A `<` directly followed by a
  letter, `/`, `!`, or `?` reads as HTML and is refused; images are refused.
- Unknown blocks, unknown fields, and anything past a limit are refused, as
  is a document over 512 KiB. Growing the block set is a Legion release.

The manager validates every answer before the SPA, the CLI, or MCP sees it,
so a misbehaving app shows an error naming the problem (`app gitops answered
view "repos" with something Legion does not render: block 2 (chart): unknown
variant ...`), never a broken page.

### Actions

An action is a button in a view's `actions` block, bound to an entry of
`ui.actions`:

| Field | Meaning |
| --- | --- |
| `name` | The action's name. |
| `title` | The default button label. |
| `confirm` | What the confirmation asks; `{namespace}`, `{job}`, `{group}`, `{task}`, and `{node}` come from the context. (Nickel's own `%{...}` would be interpolated when the manifest is evaluated, so the placeholders have no `%`.) |
| `inputs` | `{ <name> = { type, default?, values?, label?, required? } }`, typed `string`, `number`, `duration` (`30s`, `1h`), `enum` (one of `values`), or `boolean`. An input may not be named after the context (`job`, `node`, `namespace`, ...): an action acts on where it was pressed, never on a target the caller names. |
| `permission` | Required. Checked as the viewer on the context, as above. |

- An action with a `write` permission always asks first: its `confirm`, or
  "<title>?". Inputs a button does not preset are asked for in the same form.
- Running one needs `app:read` on the app and the action's permission on the
  job, task, or node it is pressed for. It does **not** need `app:write`:
  the declared permission is what the button may do, and `app:write` would
  let everyone who may press it reconfigure the app.
- The manager checks the inputs against their declarations, fills in
  defaults, and refuses unknown ones, before the app sees anything.
- Every action that passes `app:read` is recorded as an `app.action` event:
  the app, the action, the caller, the context, and the outcome (`ok`,
  `failed`, or `denied`).
- What the app then does, it does **as the viewer** when it uses the viewer
  client: the action's assertion carries a delegation of the declared
  permission on the context, and the writes are recorded by `<viewer> via
  app <app>` ([Acting as the viewer](#acting-as-the-viewer)).
- `InvokeAppAction` takes `idempotency_key`; the SPA sends one per press, so
  a retried press runs once. Actions run on the manager that leads the
  Control lane: one that reaches a follower is refused there before the app
  sees it, and replayed on the leader, so it runs once wherever it lands. A
  call with a key that is still running (a retry after a client timeout, a
  double press) waits for it, up to 45 seconds, and returns its reply,
  `deduplicated` (`Aborted`, retryable with the same key, if it still runs).
  Limitation: a leadership change while an action runs can let a retry with
  the same key run it again on the new leader.

### The viewer assertion

The manager forwards each request to one running allocation of the app's
job, over the worker plane, to the `ui.port`:

- `GET /legion/v1/views/<view>?slot=job&namespace=default&job=web`;
- `POST /legion/v1/actions/<action>` with `{ "context": {...}, "inputs":
  {...} }`.

Every request carries a **viewer assertion** in the `Legion-Viewer` header:
a JWT (ES256) signed with the cluster's workload-identity key, the same key
and JWKS as workload identity tokens.

| Claim | Value |
| --- | --- |
| `iss` | the cluster's workload-identity issuer |
| `aud` | `app:<name>` |
| `sub` | `accessor:<id>` of the viewer's token, or `principal:<name>` |
| `iat`, `exp` | issued now, expires 60 seconds later |
| `jti` | unique per request |
| `legion.io/viewer` | `{ kind: view \| action, app, name, principal, accessor?, permission?, scope: context \| any \| none, namespaces?, context, inputs?, delegation? }` |

`context` is the context exactly as the request carries it (the query, or
the body's `context`), and `inputs` is the SHA-256, in hex, of the inputs'
JSON with object keys sorted. `delegation` is on an action's assertion only:
what the app may do with it as the viewer
([Acting as the viewer](#acting-as-the-viewer)). An app **must** refuse a
request whose assertion:

- is missing, or does not verify against the cluster's JWKS;
- is for another issuer or another audience (`app:<another app>`);
- has expired (allow 30 seconds of clock skew);
- carries no `legion.io/viewer` claim (a job's own identity token could have
  `aud = app:<name>`, but never that claim: `legion.io/` claims are the
  manager's);
- names another kind, view or action, context, or inputs than the request;
- (actions) was already used.

The Rust framework does all of this before a handler runs, answering `401`
for a missing or invalid assertion and `403` for one that describes another
request. A workload that can reach the app's port directly can therefore do
nothing with it. Apps do not authorize viewers themselves: the manager
already did, against the context in the assertion. An app's one obligation is
to act only on that context.

### Acting as the viewer

An action's assertion is also the app's credential to act **as the
viewer**. It carries a delegation:

```json
"delegation": {
  "resource": "scale",
  "actions": ["write", "read"],
  "scope": { "namespace": "default", "name": "web" }
}
```

- `resource` and the first action are the action's declared `permission`.
  `read` on the same resource follows when the viewer holds it there too.
- `scope` is exactly what the manager checked: the job (`namespace`,
  `name`, and `template` when the job came from one) for the `job` and
  `task` slots; the node's `pool` for a `node` permission in the `node`
  slot; `{}` for a cluster-scoped resource; `namespace = "*"` for a
  namespaced resource in the `node` slot, which is checked across every
  namespace.

An app presents the assertion back on its own calls to Legion, in the
`legion-on-behalf-of` gRPC metadata, beside its own credential (which the
API socket attaches). The Rust framework does it for you:

```rust
let app = context.clone();
context.action("pause", move |request: ActionRequest| {
    let app = app.clone();
    async move {
        let mut legion = request.client_as_viewer(&app).await?;
        legion
            .pause_scaling(PauseScalingRequest {
                job: request.context.job_id().unwrap_or_default(),
                ..Default::default()
            })
            .await?;
        Ok(ActionOutcome::done("paused"))
    }
});
```

`client_as_viewer` dials `LEGION_ADDR` like `AppContext::client`;
`client_as_viewer_over(&connection)` does the same over a
`ManagerConnection` the app already holds (`AppContext::connection`). The
manager authenticates every call made with it as **`alice via app
<app>`**, which holds:

- the delegated resource and actions, on exactly the delegated scope, and
  nothing else: a call on another job, another resource, or another action
  is `PermissionDenied`, whatever the app's own grant allows;
- only what the app's own grant also holds there: a delegation never
  widens the app, and an action the grant lacks on the scope is dropped
  (with nothing left, the call is refused);
- nothing the app or the viewer is denied by a deny rule.

It lasts as long as the assertion (60 seconds), covers any number of unary
calls in that time, and holds only while the viewer's ACL token exists and
still grants the permission: revoking the token, or changing the policy,
ends it at the next call. The pause, pin, or job change the app makes is recorded by
`alice via app <app>`, so `legion scaling status`, `legion scaling
history`, and the event stream name the person and the app, while the
`app.action` event names who pressed. `WhoAmI` through a viewer client
reports `source = delegated` and the one rule it holds.

An app in another language does the same: keep the `Legion-Viewer` header
of an action request, and send it as `legion-on-behalf-of` metadata on its
gRPC calls to `LEGION_ADDR`.

Only the app the assertion was signed for may present it: the manager
checks that the caller's own credential is that app's `legion` identity (a
token whose job grant belongs to an allocation of `legion-system/<app>`),
so an assertion that leaks is useless to anyone else, a management token
included. Views, pages, and validate calls carry no delegation: views read
with the app's grant, after the manager checked the viewer may read.

Using the viewer client is a choice the app's code makes. An app can still
call with its own client from an action; its code is trusted with its
grant, which is why installing an app shows that grant. A well-behaved app
makes an action's writes through the viewer client, and the manifest's
declared permission is then a bound the cluster enforces.

### Writing views

A bundled app registers views and actions on the `AppContext` its body gets
(`legion app run` serves them):

```rust
context.view("job-sync", |request: ViewRequest| async move {
    let job = request.context.job_id().unwrap_or_default();
    Ok(AppView::new(vec![Block::Notice(NoticeBlock {
        tone: Tone::Info,
        title: None,
        text: format!("{job} is not deployed from a repository"),
    })]))
});
context.action("sync-now", |request: ActionRequest| async move {
    let mode = request.inputs["mode"].as_str().unwrap_or("apply");
    // ... sync request.context.job_id(), and only that job
    Ok(ActionOutcome::done(format!("synced ({mode})")))
});
```

- A handler gets the verified context and the **viewer** (principal,
  accessor, the permission checked, and where; for an action, the
  delegation). Return `AppView::hidden()` when there is nothing to show for
  a context.
- An action's writes go through `request.client_as_viewer(&context)`
  ([Acting as the viewer](#acting-as-the-viewer)); its own client
  (`context.client()`) is for reads and for work that is the app's own.
- Return `Err(Refused("...".into()).into())` from an action to refuse it
  with a message the viewer sees (`FailedPrecondition` at the caller);
  any other error is a failure.
- Views a handler builds are validated against the schema before they leave.
- `legion::apps::ui` (`UiRegistry`, `UiServer`, `AssertionVerifier`, `serve`)
  serves the same protocol for an app that is not run by `legion app run`.

The framework fetches the issuer and the JWKS through `LEGION_ADDR`, the
allocation's API socket (`/.well-known/openid-configuration` and the JWKS
path), refreshing every 5 minutes and when a key it has not seen signs an
assertion. An app in another language does the same over the socket, or
fetches the JWKS from a manager.

An action answers `200` with `{ "message": "...", "view": {...}? }`; `view`
is a refreshed view the SPA shows instead. A `4xx` answer with `{ "error":
"..." }` is reported to the caller as `FailedPrecondition` carrying the
message.

### Failures

| What the caller sees | Why | What to do |
| --- | --- | --- |
| `PermissionDenied` | no `app:read`, or not the entry's permission on this context | grant it; the app saw nothing |
| `FailedPrecondition: app views and actions need workload identity` | the manager is configured with `workloadIdentity.enabled = false`: no key to sign assertions | remove the opt-out |
| `Unavailable: app <name> has no running allocation` | the app's job runs nowhere | `legion app status <name>` |
| `Unavailable: app <name> could not be reached` | the worker could not dial the port (three allocations tried, healthy first) | the app's logs; the port's bind |
| `FailedPrecondition: app <name> refused the viewer assertion` | the app could not verify it (no `LEGION_ADDR`, no keys yet) | the app needs workload identity and its API socket |
| `FailedPrecondition: ... something Legion does not render` | the app's answer fails the schema | the message names the block and the problem |
| `NotFound` | no such app, or no such view in that slot, or no such action | `legion app view <app>` lists what it shows |
| `DeadlineExceeded` | the app took more than 10 seconds | |
| `InvalidArgument: this action cannot be delegated` | the context's job, namespace, or pool has a `*` in its name | name the target exactly |

What an app's viewer client sees from the manager (the app usually turns
these into its own refusal, which the viewer sees as `FailedPrecondition`):

| Error | Why |
| --- | --- |
| `PermissionDenied: permission denied: job:write is outside what alice via app <app> delegates` | the call is outside the delegation: another job, resource, or action |
| `PermissionDenied: legion-on-behalf-of: app <app>'s own grant does not hold scale:write on team-b/web, so it cannot act there as alice` | the viewer may, but the app's grant does not reach the context |
| `PermissionDenied: legion-on-behalf-of: alice no longer holds scale:write on default/web` | the viewer's policy changed after the press |
| `Unauthenticated: legion-on-behalf-of: the viewer's ACL token (accessor …) was revoked` | the viewer's token was deleted, or has expired |
| `Unauthenticated: legion-on-behalf-of: the viewer assertion expired at …` | more than 60 seconds since the press |
| `PermissionDenied: legion-on-behalf-of: only an app's own identity presents a viewer assertion` | the caller is not the app (a management or personal token) |
| `Unauthenticated: legion-on-behalf-of: the viewer assertion does not verify for app <app>` | another app's assertion, or not one at all |
| `PermissionDenied: legion-on-behalf-of: a delegation covers single calls …` | a stream (`StreamEvents`, `WatchScalingPolicies`, ...) was opened with it |
| `PermissionDenied: legion-on-behalf-of: … carries no delegation` | the assertion came from a manager that predates delegation (a mixed-version cluster); retry once every manager is upgraded |

## Sandboxed pages

Some UIs do not fit the block set: an editor, a graph you can drag, a log
viewer. For those an app declares **sandboxed pages**: its own single-file
HTML, which the SPA mounts in a locked-down iframe in the sidebar's **Apps**
section. A page runs the app's own JavaScript, but never in the SPA's origin,
where the operator's token lives. Its only way to Legion is a `postMessage`
**bridge** that runs the Manager RPCs the manifest lists, as the viewer, and
asks the viewer before every call that changes something.

```nickel
ui = {
  port = "api",
  pages = [
    { name = "console", title = "GitOps console", icon = "gitBranch",
      rpcs = ["ListJobs", "GetJobInfo", "RunJob"],
      permission = { resource = "job", action = "read" } },
  ],
}
```

| Field | Meaning |
| --- | --- |
| `name` | The page's name: lowercase letters, digits, `-`, `_`, `.`, at most 64. It opens at `/apps/<app>/pages/<name>`. |
| `title` | The sidebar label and the page's title. Required. |
| `icon` | A sidebar icon name; an unknown one shows a default. |
| `bundle` | Where the app serves the page, under `/legion/v1/` on its `ui.port`. Default `/legion/v1/pages/<name>`. No query, no `..`, not under `/legion/v1/views/` or `/legion/v1/actions/`. |
| `rpcs` | The Manager RPCs the page may call through the bridge, as `proto/legion.proto` spells them (`GetJobInfo`). At most 64. Default: none. |
| `permission` | `{ resource, action }`: who sees the page, checked as the viewer on any scope, like a `nav` view. Without one, `app:read` on the app suffices. |

An app declares at most 8 pages. The install and upgrade prompts list each
page with its `rpcs` (`UI: a GitOps console page (sandboxed; calls ListJobs,
GetJobInfo, RunJob as the viewer)`), so whoever installs the app sees what
its pages may do with a viewer's access.

### Which RPCs a page may call

`rpcs` is checked at install against a fixed list of the Manager RPCs pages
may call, each classified by what it does, and against what the installer
holds:

- **Reads** (`GetJobInfo`, `ListJobs`, `GetScalingStatus`, `GetKv`, ...)
  run as the viewer without asking.
- **Writes** (`RunJob`, `StopJob`, `ScaleTask`, `PutKv`, ...) are confirmed
  by the viewer every time. Classified by effect, not by ACL action:
  `RestoreInstance` needs only `snapshot:read`, but it creates an instance,
  so it is confirmed.
- **Refused**, so an install that lists one fails with the reason:
  - RPCs that return a credential or the cluster's secrets (`Login`,
    `CreateAclToken`, `CreateJoinToken`, `GetRaftJoinToken`,
    `MintExecTicket`, `CreateExecKey`, `IssueBootTicket`,
    `GetStateBackup`): a page must never hold one;
  - RPCs that change who may do what: ACL policy, user, token, exec-key,
    and grant writes (`UpsertAclPolicy`, `DeleteAclToken`, `RevokeGrant`,
    ...), namespace policies, and `DeleteJoinToken`;
  - app writes (`InstallApp`, `UpgradeApp`, `UninstallApp`,
    `SetAppEnabled`, `UpdateAppSettings`, `SetAppConfig`): an app's grant
    and pages reach other people;
  - key rotations (`RotateDataKey`, `RewrapKv`, `RotateMeshCa`);
  - break-glass operations (`IssueRaftCommand`, `ResetKeyring`,
    `RewrapRootKey`);
  - streaming RPCs (`StreamEvents`, `WatchScalingPolicies`,
    `ExecAllocation`);
  - the UI plumbing (`ListUiExtensions`, `GetAppView`, `InvokeAppAction`,
    `GetAppPage`): a page reaches its own app's views and actions with
    `legion.view` and `legion.action`;
  - worker- and app-plane RPCs (`Heartbeat`, `ReportScalingStatus`, ...).

  An RPC added to Legion later is refused until it is classified.

```
error: app gitops: ui.pages[0]: page "console": RPC "CreateAclToken" returns a credential or the cluster's secrets, which a page must never hold
```

**A page is bounded by its installer.** A page calls its RPCs as whoever
views it, an administrator included, so installing or upgrading to a page
that lists an RPC takes holding every permission behind it, **everywhere the
RPC reaches**: `job:read` in `*` and `legion-*` for `GetJobInfo`, `kv:read`
on `*` and `legion/*` for `GetKv`, `node:read` in pools `*` and `legion-*`
for `GetNodes`, and so on (`RestoreInstance` takes `snapshot:read` and
`job:write`). The check is the grant check
([Job grants](auth-and-acls.md#job-grants)), so a page can never make a
viewer do what its installer could not. In practice, installing a page with
broad RPCs takes a broad token.

```text
Error: page "console" lists GetKv, which runs as whoever views the page, so it asks for kv:read on legion/*, which your token does not hold (`*` does not reach reserved `legion-*` namespaces or `legion/` keys; only a rule that names them covers a request for them)
```

### How a page is served

The SPA fetches a page with `GetAppPage`, which the manager authorizes
exactly like a view, before anything reaches the app: `app:read` on the app,
then the page's `permission` as the viewer. It then forwards `GET <bundle>`
to one running allocation of the app's job, with a viewer assertion whose
claim has `kind = "page"`, the page's name, and the `nav` slot's empty
context ([The viewer assertion](#the-viewer-assertion)). The app answers
`200`, `text/html`, UTF-8, at most 1 MiB. The manager does not sanitize the
HTML: a page is code, confined by the browser, not by the manager.

With the Rust framework an app serves a page from `AppContext::page` (or
`UiRegistry::page`), which checks the assertion is for that page:

```rust
context.page("console", |request: PageRequest| async move {
    Ok(include_str!("console.html").to_string())
});
```

### The sandbox

The SPA mounts the bundle as `<iframe sandbox="allow-scripts" srcdoc=…>`,
and prepends (right after a leading doctype, so the page keeps standards
mode) this Content-Security-Policy and the bridge's script:

```
default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; connect-src 'none'; form-action 'none'; base-uri 'none'
```

- The frame has an **opaque origin** (no `allow-same-origin`): it cannot
  read the SPA's storage, cookies, or DOM, and has no storage of its own;
  `localStorage` throws `SecurityError`.
- It **cannot open a connection** (`connect-src 'none'`: no `fetch`,
  XHR, WebSocket, or EventSource) or **load anything** from elsewhere:
  scripts, styles, and images must be inline (`data:` images).
- It **cannot submit a form** (no `allow-forms`, and `form-action 'none'`),
  open a window (no `allow-popups`), show `alert` or `confirm` (no
  `allow-modals`), or navigate the SPA (no `allow-top-navigation`).
- A page may add its own CSP `<meta>`; policies only ever narrow.

### The bridge

The prepended script defines `window.legion`:

| | |
| --- | --- |
| `legion.call(method, request)` | Run a Manager RPC as the viewer. `request` is the request message as proto3 JSON (`{ name: "web" }`); resolves to the reply as JSON. Only RPCs in the page's `rpcs`; a write is confirmed first. |
| `legion.view(name, context)` | Render one of the app's own views (`GetAppView`), resolving to `{ title, document, actions }`. `context` is a slot context (`{ slot: "job", job: "web" }`); default `{ slot: "nav" }`. |
| `legion.action(name, inputs, context)` | Run one of the app's own actions (`InvokeAppAction`) on a `job`, `task`, or `node` context; always confirmed. Resolves to `{ message, document }`. |
| `legion.navigate(route)` | Move the SPA to an in-app route (`/jobs/default/web`). |
| `legion.theme`, `legion.onTheme(fn)` | `"light"` or `"dark"`, and a listener for changes. |
| `legion.app`, `legion.page`, `legion.rpcs` | The page's app, its name, and its allowlist. |

A refusal rejects with an `Error` whose `code` is `not_allowed` (the RPC is
not in `rpcs`), `cancelled` (the viewer said no), `invalid_argument`,
`unimplemented`, `resource_exhausted` (more than 16 calls in flight, or 4
confirmations waiting), or the gRPC status in snake case
(`permission_denied`, `not_found`, ...).

```html
<!doctype html>
<title>GitOps console</title>
<ul id="jobs"></ul>
<script>
  legion.call('ListJobs', {}).then((reply) => {
    for (const job of reply.jobs ?? []) {
      const item = document.createElement('li');
      item.textContent = job.name;
      document.getElementById('jobs').append(item);
    }
  });
</script>
```

**Every write asks.** The SPA, not the page, draws the confirmation, outside
the frame, naming the app, the RPC, and its target: `gitops wants to run
RunJob on team-a/web as you`, with the request below. The page waits for the
answer; nothing is sent until the viewer says yes.

- The target and the request shown are the **decoded message the call
  will send**, not the page's own JSON. A request that spells a field both
  ways (`nodeId` and `node_id`, which the decoder would resolve to one of
  them) is refused with `invalid_argument` and never shown.
- **Run as me** stays disabled for half a second after each confirmation
  appears, and Cancel has the focus, so a click or double-click aimed at
  something else cannot approve it. Confirmations come one at a time, each
  armed afresh.

**The allowlist is checked twice.** The SPA refuses an RPC outside `rpcs`
before anything is sent, and stamps every call it does send with the
`legion-app-page: <app>/<page>` header. The manager checks that header
against the app's **current** manifest and refuses (`PermissionDenied`) an
RPC the page does not list, so an upgrade that narrows `rpcs` holds even for
a page that is already open, and a bug in the SPA cannot widen a page's
reach. The check runs before the caller is authenticated, so every refusal
reads the same (`this sandboxed page may not make this call: ...`) and names
neither the apps installed nor a page's `rpcs`; the manager's log says
which it was. Neither check authorizes the viewer: every call is still the
viewer's own, checked against the viewer's ACLs like any other.

**Only the page is heard.** The SPA accepts a message only from the page's
own frame (its window, in the opaque origin) and only while the frame still
holds the bundle it was given: once the page navigates its frame anywhere
else, the bridge closes and the SPA says so.

### What the sandbox does not stop

A page **can leak whatever its viewer can read through its allowlisted
RPCs**. Nothing in a CSP reliably stops a frame from navigating itself to an
outside URL carrying data in the query string, so a page that calls
`GetKv` can send what it read to its author. The bridge closes when the
frame navigates, but whatever the page read before that is gone.

So treat a page's app as **trusted with its viewers' read access** through
the RPCs it lists, and read the install prompt's `rpcs` with that in mind.
That access never exceeds what its installer holds: install checks every
permission behind the page's `rpcs` against the installer, everywhere.
Writes are different: each one needs the viewer's yes, every time. And a
page never holds a token: the most it can do is what its viewer could do,
narrowed by its `rpcs`, while its viewer has it open.

### Failures

| What the viewer sees | Why | What to do |
| --- | --- | --- |
| `You may not see this page.` | no `app:read`, or not the page's `permission` | grant it; the app saw nothing |
| `... could not serve this page: ... answered ... with "application/json"; a page is served as text/html` | the app's answer is not HTML | serve `text/html` |
| `... a page is at most 1048576` | the bundle is over 1 MiB | inline less |
| `not_allowed` in the page | the RPC is not in the page's `rpcs` | add it to the manifest (and upgrade) |
| `permission_denied` in the page | the viewer lacks the RPC's permission, or the manager refused a call the page's current manifest does not allow (`this sandboxed page may not make this call`; the manager's log names the reason) | |
| `invalid_argument` in the page, `sets ... under both of its names` | the request spells a field both ways (`nodeId` and `node_id`) | send one spelling |
| install refused: `page "..." lists ..., which runs as whoever views the page, so it asks for ...` | the installer does not hold a permission behind the page's `rpcs` everywhere | have someone who does install it, or list fewer RPCs |
| `The page left the bundle ...; its bridge to Legion is closed` | the page navigated its frame | reload |

## Jobspec extensions

An app that acts on jobs usually needs settings per job: what to back up and
how often, which hostname to route, which repository a job came from. Apps
declare **keys** at fixed **extension points** of the jobspec, and job
authors write them next to what they configure, like any other field:

```nickel
{
  name = "orders-db",
  uptime = { url = "https://orders.acme.dev/healthz" },        # job: the uptime app
  task = {
    name = "postgres",
    runtime = "container",
    config = { image = "postgres:17" },
    volumes = [
      { name = "data", path = "/var/lib/postgresql",
        backup = { schedule = "0 3 * * *", keep = 14 } },       # volume: the backup app
    ],
    network = { ports = [{ name = "http", dns = { hostname = "orders.acme.dev" } }] },   # port: the dns app
  },
}
```

Values are Nickel like the rest of the jobspec (`let`, functions, imports,
and merging all work): the CLI evaluates the spec to JSON, and the manager
checks the evaluated value. `legion job spec` shows the Nickel as written.

### Extension points

| Point | Where the key is written | What the app gets as context |
| --- | --- | --- |
| `job` | the top level of the jobspec | the job |
| `group` | on **one** task of the group; it applies to the whole group (the jobspec has no group objects) | the job, the group, and the task it is written on |
| `task` | a task | the job, group, and task |
| `volume` | an entry of a task's `volumes` | the job, task, and volume name |
| `port` | an entry of a task's `network.ports` | the job, task, and port name |
| `service` | a task's `service` block | the job, task, and service name |

A `group` key written on two tasks of one group is a submit error. Points
are Legion's: an app cannot add one, and core keeps its own new fields from
colliding at these points (below).

### Declaring keys

```nickel
extensions = [
  {
    point = "volume",
    key = "backup",               # what job authors write; defaults to the app's name
    description = "Back up this volume on a schedule.",
    schema = {                    # JSON Schema, draft 7
      type = "object",
      additionalProperties = false,
      required = ["schedule"],
      properties = {
        schedule = { type = "string", description = "Cron expression, in UTC." },
        keep     = { type = "integer", minimum = 1, default = 7 },
      },
    },
    examples = [{ schedule = "0 3 * * *", keep = 14 }],
    validate = true,              # also ask the app at submit (optional)
    requires = { resource = "net", action = "write" },   # extra permission to set it (optional)
  },
],
```

Install refuses a declaration that breaks any of these:

- At most one key per point; `task` and `group` count as one point (both
  are written on a task), so an app declares at most one of the two. An app
  with several settings nests them in that key's record.
- The key is a letter, then letters, digits, `_` or `-`, at most 64. `apps`,
  `pendingExtensions`, and `extensions` are reserved everywhere.
- The schema describes an object (`type = "object"`), is at most 64 KiB, and
  uses only local `$ref`s (`"#/definitions/…"`). It is compiled at install
  with a resolver that refuses every other ref, so installing an app never
  makes the manager fetch a URL.
- Every example passes the schema (with its defaults filled), so the
  examples an app ships cannot drift from it.
- `requires` names an ACL resource and `read` or `write`.

A schema can declare `default`s. Legion fills them when a value is read, not
when it is stored ([Values as they read now](#values-as-they-read-now)).

### Keys cannot collide

1. **Not a core field.** Installing an app whose key is a field of the block
   at that point (`readOnly` on a volume, `config` on a task) fails.
2. **Not another app's key.** Installing an app whose key another installed
   app already declares at that point (`task` and `group` as one) fails,
   naming the owner. Install it under another key instead:

   ```console
   $ legion app install ./pgbackup.ncl
   Error: pgbackup declares `backup` on volumes, which the backup app already owns on volumes
   Hint: install it under another key: --key volume.backup=pgbackup
   $ legion app install ./pgbackup.ncl --key volume.backup=pgbackup
   ```

   Job authors then write `pgbackup = { … }`. The rename is part of the
   app's settings and survives upgrades.
3. **The qualified spelling never collides.** Every key can also be written
   `apps.<app>` at the same point, which no core field can take:

   ```nickel
   volumes = [{ name = "data", apps = { backup = { schedule = "0 3 * * *" } } }]
   ```

   If a later Legion release adds a core field named like an installed key,
   the core field wins at that point: the manager logs the conflict when it
   starts, `legion app status` and `legion app extensions` say so, and job
   authors write the app's values as `apps.<app>`. A submit cannot tell
   which one an author means, so it refuses, pointing at the qualified
   spelling, only when the job's previous version stored that app's value
   under the short key; any other submit takes the key as the core field.
   Stored values are untouched: they are stored by app, not by key.

### At submit

For each extension value in a `RunJob`, `RunJobTemplate`, or `PlanJob`, the
manager:

1. **finds its owner**: the installed app that declares the key, or the app
   named by `apps.<app>`, at that point. With no owner the key is unknown:
   an error, with a suggestion drawn from core fields and installed keys,
   or, with `--allow-unknown-fields`, kept as pending
   ([Unknown fields](jobs-tasks-and-scheduling.md#unknown-fields));
2. **checks it against the schema**, with the schema's defaults filled in.
   An invalid value is an error whatever the flag says;
3. **checks `requires`** (`RunJob`, `RunJobTemplate`), as the submitter,
   for a value that is new or changed since the job's previous version: on
   the job for a namespaced resource (`job`, `kv`, …), cluster-wide for a
   cluster-scoped one (`net`, `node`, …);
4. **asks the app** (`RunJob`, `RunJobTemplate`), when the key says
   `validate = true` ([The validate call](#the-validate-call));
5. **stores it as written**, by app, stamped `validated_by =
   <app>@<manifest digest>`.

```text
Error: orders-db: task postgres: volume data: backup.keep: 0 is less than the minimum of 1
Hint: fix each value above against its app's schema (`legion app extensions` shows them, `legion job schema` has them at their points)

Error: orders-db: task postgres: volume data: unknown field `bakup` (did you mean `backup`, the backup app's key?)

Error: orders-db: task postgres: port http: dns (dns app) requires net:write to set, which your token does not hold
```

A disabled app's keys are still checked at submit; nothing acts on the
values while it is off. Values are visible to anyone with `job:read`, so
they must never hold secrets: a schema should take a KV path, not the
secret. A KV reference (`$path/key`) inside a value is resolved once, at
submit, and not re-rendered when the key changes.

### Values as they read now

A job version stores each value as written. When it is read (`legion job
info`, `GetJobInfo`, the app's watch, and `legion job plan` for a spec not
yet submitted), Legion fills the defaults from the app's **current** schema
and judges the value against it, so an upgrade that changes a default
reaches every value that did not set it:

```console
$ legion job info orders-db
...
jobspec extensions:
  job: uptime = {"alert":"#oncall","url":"https://orders.acme.dev/healthz"} (uptime app; valid)
  task postgres: volume data: backup = {"keep":7,"schedule":"0 3 * * *"} (backup app; valid)
  task postgres: port http: dns = {"hostname":"orders.acme.dev"} (dns app; valid)
```

| State | Meaning |
| --- | --- |
| `valid` | the app is installed and the value passes its current schema |
| `invalid` | the value fails the current schema (an upgrade tightened it). It stays stored, the app's watch flags it, and the job's next submit must conform |
| `pending` | kept under `--allow-unknown-fields`, and no installed app claims it (or the app that does declares `requires`; below) |
| `orphaned` | its app was uninstalled, or no longer declares a key at that point. Resubmitting the job fails until the key is removed or `--allow-unknown-fields` is passed |

`detail` says why, `app disabled` while the app is off, or `adopted from
pending`. `legion job info -j` carries them under `extensions`.

### Changing only extension values restarts nothing

A resubmit whose only differences are extension values (or pending keys)
still makes a new job version, but no rollout: the running allocations move
to the new version in place, as KV re-renders and `autoscale` edits do, and
the apps see the new values through their watch.

```console
$ legion job run orders-db.ncl          # backup.keep 14 → 30
Job updated in place: only jobspec extension values changed, so nothing restarts (the apps that own them see the new values)
```

A rollout in flight, or allocations still on an older version, falls back
to a rollout, as for `autoscale`.

### How values reach the app

An app reads its values with **`WatchAppExtensions`**, a stream that only
an app's own workload identity may open (its `legion` token, which the
allocation's API socket attaches). It returns that app's values and no
other app's:

- the first message has `snapshot` set and carries every current value;
- later messages carry upserts (new, changed, a new job version, a
  validity that changed with an upgrade) and deletes (a value a resubmit
  removed, a destroyed job);
- each value has its job, namespace, version and status, its point and
  path (group, task, volume, port, service), the key as written, the value
  as written and with defaults filled, its state, and whether it was
  adopted from pending.

`shard_count` and `shards` split the values across replicas by the job id's
hash (the first eight bytes of SHA-256 of the canonical job id, modulo the
count): the same key hash the autoscaler shards by. A management token may
watch any app by naming it in `app`.

The app acts only on what carries the value: the job, task, volume, port,
or service in its context. A schema cannot name a different job.

### The validate call

A key with `validate = true` asks its app at submit, for checks a schema
cannot express (is this volume one that can be snapshotted? is this
hostname ours?). The manager sends `POST /legion/v1/extensions/validate` to
one allocation of the app's job, through the same worker-plane forward and
viewer assertion as views ([The viewer assertion](#the-viewer-assertion)),
with the submitter as the viewer, the job as the context (`job` slot), the
assertion's `kind` set to `validate`, and its inputs digest the body's:

```json
{ "job": "orders-db", "namespace": "default",
  "values": [ { "point": "volume",
                "path": { "group": "db", "task": "postgres", "volume": "data" },
                "key": "backup",
                "value": { "schedule": "0 3 * * *" },
                "effective": { "schedule": "0 3 * * *", "keep": 7 } } ] }
```

The app answers one result per value, in order:

```json
{ "results": [ { "errors": ["this volume is node-local, and backups need one that can be snapshotted"],
                 "warnings": [] } ] }
```

Errors refuse the submit; warnings pass through to the reply's `warnings`.
Each app gets one call per submit, with all its values, and two seconds:
apps are asked on the Control lane's leader, and a submit that reaches a
follower is refused there before any app is asked, then replayed on the
leader, so a submit through any manager asks once (and waits up to two
seconds, not four). **An app that cannot be reached, is disabled, answers
anything but `200`, or does not answer in time passes the values with a
warning**: a broken add-on must not block deploys. So does a value past the
end of an answer with too few results: it was not checked, and the warning
says so. The app checks values again when they reach
it through its watch, and reports problems on its own status and views.

```text
warning: orders-db: task postgres: volume data: backup was not checked by the backup app (did not answer within 2s); the schema check passed
```

On the app framework, `UiRegistry::validate(|request: ValidateRequest| …)`
answers it, with the assertion already verified; without a handler the
framework answers that every value passes.

### Installing, upgrading, uninstalling

- **Install** registers the app's keys after the collision checks, and
  **adopts pending values**: a value kept under `--allow-unknown-fields`
  before the app existed now belongs to it, checked against its schema. The
  prompt says which:

  ```text
  1 job sets backup on volumes; every value passes this version's schema.
  Adopts 1 pending value kept under --allow-unknown-fields: orders-db/postgres/data: backup
  ```

  A pending value for a key that declares `requires` is not adopted: no
  submit checked its author for the permission. It stays pending until the
  job is resubmitted by someone who holds it.
- **Upgrade** replaces the keys and schemas and reports how every stored
  value fares against the new schema. Failing values stay stored, flagged
  `invalid`:

  ```text
  14 jobs set backup on volumes; 2 fail this version's schema (flagged invalid; each job's next submit must conform):
    orders-db/postgres/data: backup: "schedule" is a required property
  ```

  A new manifest that **drops a point** jobs set a key at orphans those
  values, which is uninstall's effect on them, so it needs `--force` too,
  and the prompt lists them apart from the passing ones (`--dry-run --json`
  has them as `orphanedJobs`):

  ```text
  1 value of tier on tasks is orphaned: this version no longer declares the key there. The values stay stored, flagged orphaned, and each job's next submit fails until the key is removed:
    web/main: tier
  ```

- **Disable** keeps the keys registered, so submits are still checked;
  nothing acts on the values, and `job info` says `app disabled`.
- **Uninstall** needs `--force` while any job sets the app's keys, and
  lists them. The values stay stored, flagged `orphaned`; adopted pending
  values go back to pending.

### Discovering keys

- `legion app extensions [app]` lists every installed key: its point,
  owner, description, examples, and whether setting it needs a permission
  or asks the app. `-j` adds each key's schema.
- `legion job schema` (`GetJobSchema`, `job:read`) prints this cluster's
  jobspec schema: the core fields plus each installed key at its point,
  described as "From the backup app, 0.3.1: …", and each app under its
  point's `apps` record. Offline it prints the compiled-in schema and says
  that installed keys are not included.
- `legion job validate` checks values against the cluster's keys when a
  manager answers, and says what only a submit checks (`requires`, the
  validate call). Offline it checks core fields only and reports keys at
  extension points as not checked.
- The MCP tools `legion_job_schema` and `legion_job_validate` do the same.

## Inside an app

The app's tasks get, in their environment:

| Variable | Value |
| --- | --- |
| `LEGION_APP_NAME` | the app's name |
| `LEGION_APP_CONFIG` | the path of the app's config file (JSON), in the task's own view (`exec` and `container` tasks) |
| `LEGION_APP_BUILD` | the build marker the job was rendered for, `<buildVersion>+<buildId>` (bundled apps) |
| `LEGION_ADDR`, `LEGION_SECRETS_DIR`, `LEGION_IDENTITY_TOKEN_FILE` | the allocation's API socket and token, authenticated as the app's `legion` identity, when workload identity is on ([The API socket](workload-identity.md#control-plane-identity)) |
| `LEGION_ADDR_API`, `LEGION_PORT_API` | the app's `api` port, when its task declares one |

So `legion kv get legion/apps/gitops/state` inside the task just works,
with the app's grant and no token handling. To act as a viewer from an
action, the app's calls add the action's assertion in
`legion-on-behalf-of` ([Acting as the viewer](#acting-as-the-viewer)).

**`legion app run <name>`** is the entrypoint bundled apps run. It reads
`$LEGION_APP_CONFIG` (validated against the app's schema), serves the app's
HTTP port (`$LEGION_PORT_API` on every interface; `--listen` overrides) with
`/healthz`, which answers `200` or `503` with the health the app reports,
and the views and actions the app registers under `/legion/v1/`
([Writing views](#writing-views)), starts the app, and stops it on SIGTERM or
SIGINT, giving it 10 seconds. The bundled autoscaler declares an `api` port
and a health check on `/healthz`.

`exec` tasks may run the node's own binary with `command =
["legion:self", ...]`: the exec runtime resolves `legion:self` to the
worker's executable.

## Events and metrics

| Event | When |
| --- | --- |
| `app.installed` | an app was installed, or a bundled app seeded (`by legion:bootstrap`) |
| `app.upgraded` | a manifest was replaced: by `legion app upgrade`, or a bundled app's new build (permissions added and removed) |
| `app.uninstalled` | an app was uninstalled (grants revoked) |
| `app.enabled`, `app.disabled` | an app was enabled or disabled |
| `app.config_changed` | an app's config was replaced (the code fields it touched) |
| `app.reconciled` | the reconciler changed an app's job: `created the job`, `config changed`, `settings changed (…)`, `reverted a direct edit by <who>`, `started the job, which was stopped`, `restarted the job, which had failed (restart N)`, `resubmitted the job, which did not run healthy at version N (…)`, `stopped (app disabled)`, `stopped the job of an app that is not installed` |
| `app.reconcile_failed` | a reconcile failed (`refused` when it waits for a change), or a bundled upgrade is held (`upgrade held: …`) |
| `app.action` | someone ran an app's action: who, which, where (`meta.context`), and the outcome (`ok`, `failed`, `denied`) |

App events are scoped to the app's job, so `legion events --job
legion-system/<app>` shows them, and they reach whoever may read that job.
`legion_apps{status}` on `/metrics` counts apps by status (`running`,
`upgrading`, `degraded`, `pending`, `stopped`, and `corrupt` for records the
manager cannot decode), from the leading manager ([Metrics](metrics.md)).

## API and CLI reference

| CLI | RPC |
| --- | --- |
| `legion app list` | `ListApps` |
| `legion app status <app>` | `GetApp` |
| `legion app logs <app> [-f] [--stream S] [--tail]` | `legion job logs legion-system/<app>` |
| `legion app config get <app>`, `config schema <app>` | `GetApp` |
| `legion app config set <app> -f <file> [--dry-run]` | `SetAppConfig` |
| `legion app set <app> [--replicas N] [--placement P] [--pool P] [--cpu MHZ] [--memory MIB] [--dry-run]` | `UpdateAppSettings` |
| `legion app enable <app>`, `disable <app>` `[--dry-run]` | `SetAppEnabled` |
| `legion app install <file|url> [--config F] [--yes] [--dry-run] [--key P.K=NEW]` | `InstallApp` |
| `legion app upgrade <app> <file|url> [--config F] [--yes] [--force] [--dry-run]` | `UpgradeApp` |
| `legion app uninstall <app> [--force] [--purge] [--dry-run]` | `UninstallApp` |
| `legion app extensions [app]` | `ListApps` / `GetApp` (`App.extension_keys`) |
| `legion job schema` | `GetJobSchema` (the cluster's jobspec schema, installed keys included) |
| (an app's own values) | `WatchAppExtensions` (app-plane) |
| `legion app view <app>` | `ListUiExtensions` (what the app shows you, and where) |
| `legion app view <app> <slot> [ctx…] [--view V] [--group G]` | `GetAppView` (`job <job>`, `task <job> <task>`, `node <node>`, `nav`, `overview`) |
| (the UI's action buttons) | `InvokeAppAction` |
| (the UI's sandboxed pages) | `GetAppPage` |
| `legion app run <app> [--listen ADDR] [--config F]` | (the in-task entrypoint) |

Every command but `run` takes `--json`; `legion app view -j` returns the
validated view document, the actions it offers with whether you may run
them, and which allocation answered. The MCP server exposes
`legion_app_list`, `legion_app_status`, and `legion_app_view`
([Agent Readiness](agent-readiness.md)).
`proto/legion.proto` documents every RPC and field.

## Limitations

- **Extension values are checked against JSON Schema, plus an optional,
  fail-open call to the app.** Cross-field and cluster-aware checks are the
  app's to make, and a submit that cannot reach the app goes through with a
  warning.
- **Validity depends on the cluster.** The same jobspec can be valid on a
  cluster with an app installed and invalid on one without it
  (`--allow-unknown-fields` keeps the values pending), and two clusters
  with different apps owning one key read it differently; `apps.<app>`
  pins the owner.
- **Values share their job's lifecycle and visibility**: they are part of
  the version, visible with `job:read`, and never a place for secrets.
- **`legion job plan` and `legion job validate` do not run `requires` or
  the validate call**; only a submit does. Nickel evaluation does not know
  installed keys either (there is no importable jobspec contract yet), so a
  wrong value is caught at submit or by `legion job validate`.
- **A KV reference inside an extension value is resolved once**, at submit,
  and not re-rendered when the key changes. Like any reference, it needs
  `kv:read` on the key, checked before it is resolved
  ([KV references in a job](auth-and-acls.md#kv-references-in-a-job)).
- **Adoption is read-side.** A pending value an installed app claims is
  reported as that app's (`adopted`), but stays stored as pending until the
  job is resubmitted; uninstalling the app makes it pending again rather
  than orphaned.
- **Views and actions need workload identity**, which is on by default.
  The viewer assertion is signed with the workload-identity key, and an
  app fetches the keys that verify it through its API socket; on a manager
  configured with `workloadIdentity.enabled = false`, `GetAppView` answers
  `FailedPrecondition` and apps serve no views.
- **Sandbox apps cannot serve views yet.** A sandbox task has no API
  socket, so the framework cannot fetch the cluster's keys.
- **A sandboxed page can leak what its viewer can read** through its
  allowlisted RPCs, by navigating its own frame to a URL that carries the
  data ([What the sandbox does not stop](#what-the-sandbox-does-not-stop)).
  Install a page's app only if you would trust it with that.
- **Pages are single files in the sidebar.** No remote scripts, styles,
  fonts, or images (only `data:` images), no network, at most 1 MiB, and no
  page in a job, task, or node slot yet. They open only in the UI; the
  terminal and MCP list them without rendering them.
- **The bridge carries unary calls.** Streaming RPCs
  (`StreamEvents`, `WatchScalingPolicies`) are not available to pages.
- **Actions run from the UI.** The terminal and MCP read views and list
  their actions; there is no `legion app` command or MCP tool that runs one
  yet (`InvokeAppAction` is a public RPC).
- **Acting as the viewer is the app's choice.** An app's code can call with
  its own grant from an action handler; the declared permission bounds only
  the calls it makes through the viewer client. Install an app only if you
  would trust it with its grant.
- **A delegation is reusable for its minute.** The framework runs each
  action assertion once, but the manager accepts its delegation on any
  number of calls from the app until it expires, all bound to the same
  context and permission.
- **A delegation needs every manager on the same build.** An assertion
  signed by a manager that predates delegation carries none, so during an
  upgrade an action whose app uses the viewer client can fail (views do
  not).
- **No delegation on streams, views, or pages.** Streams refuse
  `legion-on-behalf-of`; views and pages read with the app's grant (pages
  call as the viewer through the bridge instead).
- **Views are polled, not streamed**, at the app's `ui.refresh`, never more
  often than every 5 seconds.
- **One replica answers.** A view or action goes to one running allocation
  (healthy first); an app with several replicas must answer every view from
  any of them, from state they share (KV, under its grant).
- **On `nav` and `overview`, a permission means "somewhere".** The app gets
  the viewer's namespaces for a namespaced permission and must filter rows
  itself.
- **Without workload identity, an app cannot reach Legion.** Workload
  identity is on by default; a manager configured with
  `workloadIdentity.enabled = false` runs apps whose `legion` identity gets
  no token and no API socket, so they hold no credentials; `legion app
  status` notes it, and the autoscaler's health says `idle: cannot reach
  Legion`.
- **The config file reaches `exec` and `container` tasks only.** A sandbox
  task gets `LEGION_APP_NAME` but no config file.
- **No registry.** `install` takes a file or a URL.
- **Apps live in `legion-system`.** Apps owned by a tenant namespace are not
  supported (`namespace` in a manifest is reserved).
- **A manifest's job reads KV only within the app's grant.** The
  reconciler's submit is checked against the recorded grant, so a manifest
  whose job names a KV key (a template, a reference) needs `kv:read` on it in
  its own `permissions`; otherwise the submit is refused and the app shows
  `reconcile refused`.
- **App uids are a fixed range.** Apps' `exec` tasks run as uids from
  `1900000000` up, with no passwd entry; a host whose own users sit in that
  range would share them. Container tasks get their own per-allocation
  user namespace instead, and sandbox tasks their VM.
- **No host-level constructs in an app's job, even for an admin.** An app
  that would need an `exec` health check, a node file, or the node's
  identity cannot be an app; run it as an ordinary job.
- **Backoff is the leader's memory.** A new leader (or the same manager
  leading again) retries a failing or refused app once, then backs off from
  scratch.
- **No rollback verb.** The record keeps only the installed manifest: to go
  back, `legion app upgrade` with the previous manifest file.
- **An app's job is not autoscaled.** Its count is the app's `replicas`;
  `ScaleTask` on it is refused, the autoscaler's included.
- **Reverting is gone; refusing replaced it.** A tool that syncs
  `legion-system` jobs directly gets `FailedPrecondition` for installed
  apps' jobs: drive apps through `legion app` (or `InstallApp`,
  `UpdateAppSettings`, `SetAppConfig`) instead.
- **`config get` prints JSON**, not Nickel. JSON is valid input for `config
  set`.
- **Uninstall leaves the stopped job, and the app's KV.** `legion job
  destroy legion-system/<app>` removes the job; `--purge` removes the KV
  under `legion/apps/<app>/` (or `legion kv delete` later).
- **A worker hashes its executable at startup** for its build id: under a
  second for a release binary, longer for a debug build.
- **Stopping a manager leaves its app processes running.** Like any task,
  an app on a manager's system worker outlives the worker on purpose, and
  the manager's next start re-adopts it. To take a manager down for good,
  drain its system worker first (`legion node drain <node>`), or `legion
  app disable` the app.
