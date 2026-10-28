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
   (stored now, resolved by a later release; see
   [Limitations](#limitations)).

A **manifest** declares all three. An **app record**, stored on the
managers, holds the installed manifest, the operator's settings and config,
and the grant. The manager's **app reconciler** keeps the job matching the
record: it submits the job, re-renders it when the record changes, reverts
direct edits to it, and restarts it when someone stops it.

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
`ScaleTask`, reporting each evaluation back to the manager. Its grant is
`scale:read` and `scale:write` in every namespace, and `node:read` for its
node-group panel: it can resize, not redeploy. Its config takes `interval`
(default `"15s"`) and `mode` (`"dry-run"` forces every policy into dry-run).
When it stops reporting, `legion scaling status` says `NOT RUNNING`. Its UI
is built from app views: an Autoscaling page in the sidebar, a panel on
every autoscaled job's page, a badge on each autoscaled task, and a
node-group panel on node pages
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
  extensions = [ ... ],                  # jobspec keys (stored; see Limitations)
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
| `job` | A jobspec without `name` or `namespace`, which Legion sets. Must define `task` or `tasks`, and parse as a jobspec. |
| `permissions` | ACL rules in the shape `legion acl policy apply` takes: what the app's `legion` identity asks to be granted. |
| `ui` | UI registrations: the slots the app's views appear in, the actions they offer, the port they are served on, and the refresh interval ([Views and actions](#views-and-actions)). Unknown fields are errors. |
| `extensions` | Jobspec key declarations. Validated as a list and stored. |
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
- an `exec` task that names no `user` runs as `systemWorker.appUser`
  (default `nobody`). Tasks in `legion-system` may not run as root
  ([Namespaces](namespaces.md#reserved-namespaces));
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
gitops 0.2.0 runs its own code (1 artifact) on your managers, as user nobody,
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
`--json`), `--dry-run` stops after the prompt, and `--config <file>` sets
the initial config.

**You can grant only what you hold.** Installing needs `app:write` on the
app's name, and every permission the manifest asks for, checked exactly as a
job submit checks a job's `permissions`
([Job grants](auth-and-acls.md#job-grants)). The grant is recorded on the app
record in the installer's name:

```text
Error: app gitops 0.2.0 asks for job:write in team-*, which your token does not hold
Hint: have someone who holds it do this, or ask for it to be granted to you
```

A bundled app's name cannot be installed, and an installed name cannot be
installed again (`legion app upgrade` replaces a manifest).

### Upgrading

```console
$ legion app upgrade gitops ./gitops-0.3.0.ncl
gitops 0.3.0 runs its own code (1 artifact) on your managers, as user nobody, ...
Permission changes:
  + net       read
  - template  read
Approve the new permissions and upgrade? [y/N] y
upgraded gitops 0.2.0 → 0.3.0
```

A new manifest is new code running with the grant, so upgrading needs
`app:write` and every permission of the **new** manifest, as installing
does. When the new manifest asks for anything the installed one did not, the
upgrade needs approval (`--yes`, or the question); one that only keeps or
drops permissions upgrades without asking. The stored config must match the
new schema; `--config <file>` replaces it in the same step. `--dry-run`
shows the diff and changes nothing. Bundled apps cannot be upgraded this
way.

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

### Settings, enable, disable

```console
$ legion app set autoscaler --replicas 2 --placement any
autoscaler: settings changed (replicas 1 → 2, placement managers → any)
$ legion app set autoscaler --cpu 500 --memory 512    # per task; 0 clears an override
$ legion app disable autoscaler     # stops the job; keeps the record, config, and grant
$ legion app enable autoscaler
```

`--pool` picks the worker pool for `workers` and `any` placement. Enabling
an app that is already enabled is not a no-op: it is a record write, which
retries a reconcile a refusal parked ([Failures](#failures)).

### Uninstalling

```console
$ legion app uninstall gitops
uninstalled gitops; job legion-system/gitops stopped and 3 grant(s) revoked (`legion job destroy legion-system/gitops` removes the job)
```

Uninstall disables the app, stops its job, revokes the job's grants (its
workload tokens lose them at their next request), and deletes the record. The
stopped job stays until someone destroys it. Bundled apps cannot be
uninstalled; disable them.

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
re-renders the same spec. Any other write that stores a version (`RunJob`,
`RunJobTemplate`) clears it.

**Direct edits are reverted.** A direct submit of an app's job clears the
stamp, and its job event wakes the reconciler, so the edit is reverted within
seconds, and the event says by whom:

```console
$ legion job info legion-system/autoscaler
...
managed by app autoscaler (digest 3f9a12c4…); direct edits are reverted. Use `legion app`.

$ legion events --kind app.reconciled
app.reconciled  App autoscaler: reverted a direct edit by userpass:alice (from legion-system/autoscaler-7)
```

Submitting an app's job directly still needs everything the app's grant
holds (a submit always checks the job's `permissions` against the
submitter), so a token with `job:write` on `legion-system` alone cannot
launder the app's grant by editing its job.

**Exactly once per transition.** Each submit carries the idempotency key
`app:<name>:<digest>:<from>`, where `<from>` is the state it starts from (the
job's latest version, `none`, or `stopped-<version>`). If the leader dies
after its submit lands but before it records that, the next leader's pass
finds the stamp and does nothing, and a retry of the same key is
deduplicated. A new transition (reverting the same edit a second time,
enabling within a day of disabling) starts from another state and gets a new
key.

**Triggers.** A pass runs for an app when its record changes (any `legion
app` write), when its job changes (a new version, a stop), when a node joins
or re-registers (bundled apps' build markers), when a manager takes over the
Control lane (it also seeds bundled apps then), and every 60 seconds for
every app, in case an event was missed. Bursts coalesce: changes that arrive
during a pass queue one more pass, not one each.

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
  namespace quota, say).

### Status

`legion app list`, `legion app status`, and `GetApp` derive an app's status
when they are read:

| Status | When |
| --- | --- |
| `stopped` | the app is disabled |
| `degraded` | the last reconcile failed, or fewer allocations run healthy than it asks for (`2/3 running`) |
| `pending` | nothing runs yet: no build marker, no job yet, the job was stopped and is being restarted, or the scheduler's reason it is blocked (`no room in pool legion-system`) |
| `running` | every allocation it asks for runs and reports healthy (its health checks pass) |

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
  when every ready node its placement allows reports the same one. Nodes that
  report `unknown`, an unparsable version, or no build id are left out.
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
when the leading manager itself runs the marker's build, since it can read
only manifests compiled into its own binary; until then it renders from the
manifest stored on the record. New permissions are recorded as granted by
`legion:bootstrap` and reported in the `app.upgraded` event. A stored config
that no longer matches the new schema is kept, and reported in the app's
status.

Limits:

- **"Once" counts the reconciler's own restarts.** A manager restarting for
  its upgrade still kills the allocations on its system worker, and the
  scheduler places them again, possibly on a manager that has not upgraded.
- **Pre-releases.** `version_matches` uses semver requirements, under which a
  pre-release of a newer version (`0.16.0-rc.1`) does not satisfy
  `>=0.15.0`. During a mixed window, such nodes cannot host the app; once
  every eligible node runs the pre-release, the marker moves to it.

## Trust model

- **An app's power is its grant**, and the grant is never more than a person
  held: the installer (checked at install), an upgrader (checked at
  upgrade), or whoever changes a code field (checked at `config set`).
  Bundled apps are the exception: their grants are recorded by
  `legion:bootstrap` at seed time, without a check, because the manifest is
  part of the binary you chose to run, which is already the manager.
- **`app:write` is not the grant.** It changes settings, config outside code
  fields, and enabled state; it cannot add code to an app.
- **Apps run unprivileged.** Tasks in `legion-system` may not run as root,
  and privileged containers are refused there
  ([Namespaces](namespaces.md#reserved-namespaces)). Root on a manager could
  read the raft state and the data directory, so an app's blast radius stays
  its grant.
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
  ([What the sandbox does not stop](#what-the-sandbox-does-not-stop)), so a
  page's app is trusted with its viewers' read access.

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
| `InstallApp` | `app:write` on the app, and every permission the manifest asks for |
| `UpgradeApp` | `app:write`, and every permission the new manifest asks for |
| `UninstallApp`, `SetAppEnabled`, `UpdateAppSettings` | `app:write` |
| `SetAppConfig` | `app:write`, and the app's whole grant when a code field changes |
| `ListUiExtensions` | `app:read` (filtered by `names`, and by each entry's permission) |
| `GetAppView` | `app:read` on the app, and the entry's permission on the context |
| `InvokeAppAction` | `app:read` on the app, and the action's permission on the context |
| `GetAppPage` | `app:read` on the app, and the page's permission on any scope |

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
- `InvokeAppAction` takes `idempotency_key`; the SPA sends one per press, so
  a retried press runs once.

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
| `legion.io/viewer` | `{ kind: view \| action, app, name, principal, accessor?, permission?, scope: context \| any \| none, namespaces?, context, inputs? }` |

`context` is the context exactly as the request carries it (the query, or
the body's `context`), and `inputs` is the SHA-256, in hex, of the inputs'
JSON with object keys sorted. An app **must** refuse a request whose
assertion:

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
  accessor, the permission checked, and where). Return `AppView::hidden()`
  when there is nothing to show for a context.
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
may call, each classified by what it does:

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

**The allowlist is checked twice.** The SPA refuses an RPC outside `rpcs`
before anything is sent, and stamps every call it does send with the
`legion-app-page: <app>/<page>` header. The manager checks that header
against the app's **current** manifest and refuses (`PermissionDenied`) an
RPC the page does not list, so an upgrade that narrows `rpcs` holds even for
a page that is already open, and a bug in the SPA cannot widen a page's
reach. The manager logs each such call with the page. Neither check
authorizes the viewer: every call is still the viewer's own, checked
against the viewer's ACLs like any other.

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
| `permission_denied` in the page | the viewer lacks the RPC's permission, or the manager refused an RPC the current manifest does not list | |
| `The page left the bundle ...; its bridge to Legion is closed` | the page navigated its frame | reload |

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
with the app's grant and no token handling.

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
| `app.reconciled` | the reconciler changed an app's job: `created the job`, `config changed`, `settings changed (…)`, `reverted a direct edit by <who>`, `started the job, which was stopped`, `stopped (app disabled)` |
| `app.reconcile_failed` | a reconcile failed (`refused` when it waits for a change) |
| `app.action` | someone ran an app's action: who, which, where (`meta.context`), and the outcome (`ok`, `failed`, `denied`) |

App events are scoped to the app's job, so `legion events --job
legion-system/<app>` shows them, and they reach whoever may read that job.
`legion_apps{status}` on `/metrics` counts apps by status, from the leading
manager ([Metrics](metrics.md)).

## API and CLI reference

| CLI | RPC |
| --- | --- |
| `legion app list` | `ListApps` |
| `legion app status <app>` | `GetApp` |
| `legion app logs <app> [-f] [--stream S] [--tail]` | `legion job logs legion-system/<app>` |
| `legion app config get <app>`, `config schema <app>` | `GetApp` |
| `legion app config set <app> -f <file>` | `SetAppConfig` |
| `legion app set <app> [--replicas N] [--placement P] [--pool P] [--cpu MHZ] [--memory MIB]` | `UpdateAppSettings` |
| `legion app enable <app>`, `disable <app>` | `SetAppEnabled` |
| `legion app install <file|url> [--config F] [--yes] [--dry-run]` | `InstallApp` |
| `legion app upgrade <app> <file|url> [--config F] [--yes] [--dry-run]` | `UpgradeApp` |
| `legion app uninstall <app>` | `UninstallApp` |
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

- **Jobspec extensions are stored, not used.** The manifest's `extensions`
  are validated for shape and kept on the record; resolving jobspec keys
  comes in a later release.
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
- **Managers should agree on `systemWorker.appUser`.** The leading manager's
  value is rendered into exec tasks, so a manager with another value renders
  a different job, and resubmits the app when it takes over.
- **Backoff is the leader's memory.** A new leader retries a failing or
  refused app once, then backs off from scratch.
- **`config get` prints JSON**, not Nickel. JSON is valid input for `config
  set`.
- **Uninstall leaves the stopped job.** `legion job destroy
  legion-system/<app>` removes it.
- **A worker hashes its executable at startup** for its build id: under a
  second for a release binary, longer for a debug build.
- **Stopping a manager leaves its app processes running.** Like any task,
  an app on a manager's system worker outlives the worker on purpose, and
  the manager's next start re-adopts it. To take a manager down for good,
  drain its system worker first (`legion node drain <node>`), or `legion
  app disable` the app.
